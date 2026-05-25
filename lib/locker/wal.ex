defmodule Locker.WAL do
  @moduledoc """
  Write-Ahead Log (WAL) GenServer for crash-safe persistence.

  All state mutations are written to the WAL before being applied to
  in-memory structures. Uses a custom binary format with CRC32 integrity
  checks and automatic segment rollover.

  ## Configuration

  Start with a type registry mapping atom types to byte identifiers:

      types = %{keyset_created: 0x01, proof_spent: 0x02, checkpoint: 0x06}

      {:ok, _pid} = Locker.WAL.start_link(
        wal_dir: "/data/wal",
        types: types,
        max_segment_bytes: 64 * 1024 * 1024
      )

  ## Binary Format

  Each segment file begins with a header:

      | Field       | Size    | Description                        |
      |-------------|---------|-----------------------------------|
      | magic       | 4 bytes | "LWAL" magic bytes                |
      | version     | 2 bytes | Big-endian uint16, format version |
      | entry_count | 4 bytes | Big-endian uint32, entry count    |

  Followed by entries in the format defined by `Locker.WAL.Entry`.

  ## Segment Naming

  Segments are named with a monotonic index: `wal.000001`, `wal.000002`, etc.

  ## Security

  WAL segments are not encrypted at rest. Segment files are created
  with `0600` permissions (owner-only). Callers writing sensitive
  payloads (key material, credentials) must encrypt before passing
  to `append/2`.
  """

  use GenServer

  alias Locker.WAL.Entry

  require Logger

  @magic "LWAL"
  @version 1
  @header_size 10
  @default_max_segment_bytes 64 * 1024 * 1024
  @max_rotation_retries 3
  @max_queue_depth 500
  @max_read_bytes 128 * 1024 * 1024

  # --- Public API ---

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc "Appends an entry to the WAL. The write is fsynced before returning."
  @spec append(GenServer.server(), Entry.t()) :: :ok | {:error, term()}
  def append(server \\ __MODULE__, %Entry{} = entry) do
    with :ok <- check_overloaded(server) do
      GenServer.call(server, {:append, entry})
    end
  end

  @doc """
  Returns all valid entries from all WAL segments.

  Accepts an optional timeout (default 30s). Large WALs with millions
  of entries may need a longer timeout; for recovery scenarios use
  `replay/3` directly instead of going through the GenServer.
  """
  @spec read_all(GenServer.server(), timeout()) :: {:ok, [Entry.t()]} | {:error, term()}
  def read_all(server \\ __MODULE__, timeout \\ 30_000) do
    GenServer.call(server, :read_all, timeout)
  end

  @doc "Writes a checkpoint entry marking all current entries as committed."
  @spec checkpoint(GenServer.server()) :: :ok | {:error, term()}
  def checkpoint(server \\ __MODULE__) do
    GenServer.call(server, :checkpoint)
  end

  @doc "Forces rotation to a new segment file."
  @spec rotate(GenServer.server()) :: :ok | {:error, term()}
  def rotate(server \\ __MODULE__) do
    GenServer.call(server, :rotate)
  end

  @doc "Returns WAL state info: current segment, entry count, bytes written."
  @spec info(GenServer.server()) :: map()
  def info(server \\ __MODULE__) do
    GenServer.call(server, :info)
  end

  @doc """
  Reads all valid entries from WAL segments on disk without a running GenServer.

  Used by recovery modules to replay WAL entries before the supervision
  tree is fully started.

  ## Options

  * `:skip_corrupt`: skip corrupt segments with a warning (default: false)

  ## Returns

  * `{:ok, entries, corrupt_count}` on success
  * `{:error, {:corrupt_segment, path, reason}}` if corrupt and
    `skip_corrupt` is false
  """
  @spec replay(binary(), %{non_neg_integer() => atom()}, keyword()) ::
          {:ok, [Entry.t()], non_neg_integer()}
          | {:error, {:corrupt_segment, binary(), term()}}
  def replay(wal_dir, byte_map, opts \\ []) do
    validate_path!(wal_dir)
    skip_corrupt? = Keyword.get(opts, :skip_corrupt, false)

    wal_dir
    |> list_segments()
    |> Enum.reduce_while({[], 0}, fn segment_path, {segments_acc, corrupt_acc} ->
      case read_segment_entries(segment_path, byte_map) do
        {:ok, entries, 0} ->
          {:cont, {[entries | segments_acc], corrupt_acc}}

        {:ok, entries, corrupt_count} when skip_corrupt? ->
          Logger.warning("WAL: #{corrupt_count} corrupt entries in #{segment_path}, skipping")
          {:cont, {[entries | segments_acc], corrupt_acc + corrupt_count}}

        {:ok, _entries, corrupt_count} ->
          {:halt, {:error, {:corrupt_segment, segment_path, {:corrupt_entries, corrupt_count}}}}

        {:error, reason} when skip_corrupt? ->
          Logger.warning("WAL: skipping corrupt segment #{segment_path}: #{inspect(reason)}")
          {:cont, {segments_acc, corrupt_acc + 1}}

        {:error, reason} ->
          {:halt, {:error, {:corrupt_segment, segment_path, reason}}}
      end
    end)
    |> case do
      {:error, _reason} = err ->
        err

      {segments_acc, corrupt_count} ->
        # Reverse-then-flatten: O(n) instead of quadratic ++ accumulation
        entries = segments_acc |> Enum.reverse() |> List.flatten()
        {:ok, entries, corrupt_count}
    end
  end

  @doc """
  Prunes old WAL segments, keeping the `keep` most recent.

  Returns `{:ok, pruned_count}`.
  """
  @spec prune(binary(), non_neg_integer()) :: {:ok, non_neg_integer()}
  def prune(wal_dir, keep \\ 5) when keep >= 1 do
    validate_path!(wal_dir)
    segments = wal_dir |> list_segments() |> Enum.sort()
    to_prune = Enum.drop(segments, -keep)
    Enum.each(to_prune, &File.rm/1)
    {:ok, length(to_prune)}
  end

  @doc "Truncates all WAL segments, removing all data."
  @spec truncate(binary()) :: :ok
  def truncate(wal_dir) do
    validate_path!(wal_dir)
    Enum.each(list_segments(wal_dir), &File.rm/1)
    :ok
  end

  # --- GenServer Callbacks ---

  @impl GenServer
  def init(opts) do
    Process.flag(:trap_exit, true)

    wal_dir = Keyword.fetch!(opts, :wal_dir)
    validate_path!(wal_dir)

    type_map = Keyword.fetch!(opts, :types)
    max_segment = Keyword.get(opts, :max_segment_bytes, @default_max_segment_bytes)

    validate_type_map!(type_map)
    byte_map = Map.new(type_map, fn {k, v} -> {v, k} end)
    File.mkdir_p!(wal_dir)

    {segment_index, entry_count, bytes_written} = scan_existing_segments(wal_dir, byte_map)
    segment_path = segment_path(wal_dir, segment_index)

    case open_or_create_segment(segment_path) do
      {:ok, fd, actual_bytes} ->
        {:ok,
         %{
           wal_dir: wal_dir,
           fd: fd,
           segment_index: segment_index,
           segment_path: segment_path,
           entry_count: entry_count,
           bytes_written: max(bytes_written, actual_bytes),
           writes_halted: false,
           type_map: type_map,
           byte_map: byte_map,
           max_segment_bytes: max_segment
         }}

      {:error, reason} ->
        {:stop, {:wal_open_failed, reason}}
    end
  end

  @impl GenServer
  def handle_call({:append, _entry}, _from, %{writes_halted: true} = state) do
    {:reply, {:error, :writes_halted}, state}
  end

  def handle_call({:append, entry}, _from, state) do
    bin = Entry.encode(entry, state.type_map)

    with :ok <- :file.write(state.fd, bin),
         :ok <- :file.sync(state.fd) do
      new_bytes = state.bytes_written + byte_size(bin)
      new_count = state.entry_count + 1

      new_state = %{state | bytes_written: new_bytes, entry_count: new_count}
      new_state = maybe_rotate(new_state)
      update_header_count_with_retry(new_state)
      {:reply, :ok, new_state}
    else
      {:error, reason} ->
        Logger.error("WAL: write/fsync failed: #{inspect(reason)}")
        {:reply, {:error, {:wal_write_failed, reason}}, state}
    end
  end

  def handle_call(:read_all, _from, state) do
    case replay(state.wal_dir, state.byte_map, skip_corrupt: true) do
      {:ok, entries, _corrupt_count} ->
        {:reply, {:ok, entries}, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call(:checkpoint, _from, %{writes_halted: true} = state) do
    {:reply, {:error, :writes_halted}, state}
  end

  def handle_call(:checkpoint, _from, state) do
    checkpoint_entry = %Entry{
      type: :checkpoint,
      payload: %{
        timestamp: System.system_time(:millisecond),
        entry_count: state.entry_count,
        segment_index: state.segment_index
      }
    }

    bin = Entry.encode(checkpoint_entry, state.type_map)

    case :file.write(state.fd, bin) do
      :ok ->
        :file.sync(state.fd)

        new_state = %{
          state
          | bytes_written: state.bytes_written + byte_size(bin),
            entry_count: state.entry_count + 1
        }

        update_header_count_with_retry(new_state)
        {:reply, :ok, new_state}

      {:error, reason} ->
        {:reply, {:error, {:checkpoint_failed, reason}}, state}
    end
  end

  def handle_call(:rotate, _from, state) do
    case do_rotate(state) do
      {:ok, new_state} -> {:reply, :ok, new_state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call(:info, _from, state) do
    {:reply,
     %{
       wal_dir: state.wal_dir,
       segment_index: state.segment_index,
       segment_path: state.segment_path,
       entry_count: state.entry_count,
       bytes_written: state.bytes_written
     }, state}
  end

  @impl GenServer
  def handle_info(_msg, state) do
    {:noreply, state}
  end

  @impl GenServer
  def terminate(_reason, state) do
    if state[:fd] do
      try do
        :file.sync(state.fd)
        :file.close(state.fd)
      catch
        _kind, _reason -> :ok
      end
    end

    :ok
  end

  # --- Private ---

  defp check_overloaded(server) when is_pid(server) do
    case Process.info(server, :message_queue_len) do
      {:message_queue_len, len} when len > @max_queue_depth ->
        {:error, :wal_overloaded}

      nil ->
        {:error, :not_running}

      _other ->
        :ok
    end
  end

  defp check_overloaded(server) when is_atom(server) do
    case Process.whereis(server) do
      nil -> {:error, :not_running}
      pid -> check_overloaded(pid)
    end
  end

  defp maybe_rotate(%{bytes_written: bytes, max_segment_bytes: max} = state)
       when bytes >= max do
    rotation_failures = Map.get(state, :rotation_failures, 0)

    case do_rotate(state) do
      {:ok, rotated_state} ->
        Map.delete(rotated_state, :rotation_failures)

      {:error, reason} ->
        failures = rotation_failures + 1

        Logger.warning(
          "WAL: segment rotation failed (attempt #{failures}/#{@max_rotation_retries}): #{inspect(reason)}"
        )

        if failures >= @max_rotation_retries do
          Logger.error("WAL: #{failures} consecutive rotation failures, halting writes")
          Map.merge(state, %{rotation_failures: failures, writes_halted: true})
        else
          Map.put(state, :rotation_failures, failures)
        end
    end
  end

  defp maybe_rotate(state), do: state

  defp do_rotate(state) do
    new_index = state.segment_index + 1
    new_path = segment_path(state.wal_dir, new_index)

    case open_or_create_segment(new_path) do
      {:ok, fd, bytes} ->
        try do
          :file.sync(state.fd)
        after
          :file.close(state.fd)
        end

        new_state = %{
          state
          | fd: fd,
            segment_index: new_index,
            segment_path: new_path,
            entry_count: 0,
            bytes_written: bytes
        }

        update_header_count_with_retry(new_state)
        {:ok, new_state}

      {:error, reason} ->
        {:error, {:rotate_failed, reason}}
    end
  end

  defp open_or_create_segment(path) do
    if File.exists?(path) do
      open_existing_segment(path)
    else
      create_new_segment(path)
    end
  end

  defp open_existing_segment(path) do
    case :file.open(String.to_charlist(path), [:raw, :binary, :read, :write]) do
      {:ok, fd} ->
        case :file.read(fd, @header_size) do
          {:ok, <<@magic, @version::unsigned-big-16, header_count::unsigned-big-32>>} ->
            {:ok, pos} = :file.position(fd, :eof)

            if pos > @header_size and header_count == 0 do
              Logger.warning(
                "WAL: segment #{path} has entries but header count is 0; header may be stale"
              )
            end

            {:ok, fd, pos}

          _other ->
            :file.close(fd)
            create_new_segment(path)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp create_new_segment(path) do
    header = <<@magic, @version::unsigned-big-16, 0::unsigned-big-32>>

    case :file.open(String.to_charlist(path), [:raw, :binary, :read, :write]) do
      {:ok, fd} ->
        # Restrict permissions to owner only (0600)
        File.chmod(path, 0o600)
        :ok = :file.write(fd, header)
        :file.sync(fd)
        {:ok, fd, @header_size}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp update_header_count(state) do
    with {:ok, _pos} <- :file.position(state.fd, 6),
         :ok <- :file.write(state.fd, <<state.entry_count::unsigned-big-32>>),
         :ok <- :file.sync(state.fd),
         {:ok, _eof_pos} <- :file.position(state.fd, :eof) do
      :ok
    else
      _error -> :error
    end
  end

  defp update_header_count_with_retry(state) do
    case update_header_count(state) do
      :ok -> :ok
      :error -> update_header_count(state)
    end
  end

  defp segment_path(wal_dir, index) do
    Path.join(wal_dir, IO.iodata_to_binary(:io_lib.format("wal.~6..0B", [index])))
  end

  defp scan_existing_segments(wal_dir, byte_map) do
    case list_segments(wal_dir) do
      [] ->
        {1, 0, 0}

      segments ->
        last_segment = List.last(segments)
        index = parse_segment_index(last_segment)
        count_last_segment(last_segment, index, byte_map)
    end
  end

  defp count_last_segment(segment_path, index, byte_map) do
    case File.stat(segment_path) do
      {:ok, %{size: size}} ->
        count =
          case read_segment_entries(segment_path, byte_map) do
            {:ok, entries, _corrupt} -> length(entries)
            {:error, _reason} -> 0
          end

        {index, count, size}

      {:error, _reason} ->
        {index, 0, 0}
    end
  end

  defp list_segments(wal_dir) do
    case File.ls(wal_dir) do
      {:ok, files} ->
        files
        |> Enum.filter(&Regex.match?(~r/^wal\.\d+$/, &1))
        |> Enum.sort()
        |> Enum.map(&Path.join(wal_dir, &1))

      {:error, _reason} ->
        []
    end
  end

  defp parse_segment_index(path) do
    path
    |> Path.basename()
    |> String.replace_prefix("wal.", "")
    |> String.to_integer()
  end

  defp validate_path!(path) do
    if String.contains?(path, "..") do
      raise ArgumentError, "path must not contain traversal (..): #{path}"
    end
  end

  defp validate_type_map!(type_map) do
    bytes = Map.values(type_map)

    if length(bytes) != length(Enum.uniq(bytes)) do
      raise ArgumentError,
            "WAL type_map has duplicate byte values; each type must map to a unique byte"
    end

    unless Map.has_key?(type_map, :checkpoint) do
      raise ArgumentError,
            "WAL type_map must include :checkpoint type (required for checkpoint/1)"
    end
  end

  defp read_segment_entries(path, byte_map) do
    case File.stat(path) do
      {:ok, %{size: size}} when size > @max_read_bytes ->
        {:error, {:segment_too_large, size}}

      _stat ->
        case File.read(path) do
          {:ok, <<@magic, @version::unsigned-big-16, _count::unsigned-big-32, data::binary>>} ->
            {entries, corrupt_count} = Entry.decode_all(data, byte_map)
            {:ok, entries, corrupt_count}

          {:ok, _other} ->
            {:error, :invalid_header}

          {:error, reason} ->
            {:error, reason}
        end
    end
  end
end
