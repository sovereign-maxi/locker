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

  ## Write mode

  Two modes control how `append/2` interacts with disk:

  * `mode: :sync` (default) — every `append/2` writes and fsyncs
    before returning. Durable across an OS crash; throughput is
    bounded by fsync latency.
  * `mode: :buffered` — `append/2` enqueues into an in-memory buffer
    and returns immediately. The buffer flushes to disk with a
    single fsync when EITHER `flush_interval_ms` elapses OR
    `flush_max_entries` accumulate. Callers that need guaranteed
    durability on a specific write use `append_sync/2` (drains the
    buffer, writes the entry, fsyncs) or call `flush/1` explicitly.

  Buffered mode extra options (defaults shown):

      mode: :buffered,
      flush_interval_ms: 500,
      flush_max_entries: 100

  A flush failure (write or fsync error) in `:buffered` mode is
  fail-hard: the WAL GenServer stops with `{:flush_failed, reason}`
  so the supervisor sees it. Entries already acknowledged as `:ok`
  but not yet flushed are unrecoverable — this is inherent to the
  write-behind tradeoff and callers who require per-entry
  durability guarantees must use `append_sync/2`.

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
  @default_flush_interval_ms 500
  @default_flush_max_entries 100
  @max_rotation_retries 3
  @max_queue_depth 500
  @max_read_bytes 128 * 1024 * 1024

  # --- Public API ---

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Appends an entry to the WAL. In `:sync` mode (default) the write
  is fsynced before returning. In `:buffered` mode the entry is
  enqueued and `:ok` is returned immediately; the buffer flushes on
  its own schedule.

  `timeout` bounds the GenServer call (default 5s). Writers on
  money-commit paths should pass a generous timeout so a slow
  fsync stalls the caller instead of exiting it mid-commit.
  """
  @spec append(GenServer.server(), Entry.t(), timeout()) :: :ok | {:error, term()}
  def append(server \\ __MODULE__, %Entry{} = entry, timeout \\ 5_000) do
    with :ok <- check_overloaded(server) do
      GenServer.call(server, {:append, entry}, timeout)
    end
  end

  @doc """
  Appends an entry and guarantees it (plus every prior buffered
  entry) is on disk and fsynced before returning. Behaves identically
  to `append/2` in `:sync` mode; in `:buffered` mode drains the
  buffer first. Accepts the same optional call timeout as `append/3`.
  """
  @spec append_sync(GenServer.server(), Entry.t(), timeout()) :: :ok | {:error, term()}
  def append_sync(server \\ __MODULE__, %Entry{} = entry, timeout \\ 5_000) do
    with :ok <- check_overloaded(server) do
      GenServer.call(server, {:append_sync, entry}, timeout)
    end
  end

  @doc """
  Appends a batch of entries ATOMICALLY: every entry is encoded
  upfront, then the whole batch lands with a single write + fsync
  (in `:sync` mode) or as one enqueued group (in `:buffered` mode,
  flushed per the WAL's own schedule). Either every entry in the
  batch becomes durable or none does — a mid-batch failure can
  never leave a durable prefix behind.

  Returns `:ok` or `{:error, reason}`. An encoding failure on any
  entry (unknown type, oversized payload) aborts the whole batch
  before anything is written. A write/fsync failure returns
  `{:error, {:wal_write_failed, reason}}`; because the batch is a
  single write, a failure there can at worst leave a torn byte
  tail (flagged as corruption by `read_all_strict/1`), never a
  valid partial entry.

  `timeout` bounds the GenServer call (default 5s).
  """
  @spec append_batch(GenServer.server(), [Entry.t()], timeout()) :: :ok | {:error, term()}
  def append_batch(server \\ __MODULE__, entries, timeout \\ 5_000) when is_list(entries) do
    with :ok <- check_overloaded(server) do
      GenServer.call(server, {:append_batch, entries}, timeout)
    end
  end

  @doc """
  Forces a flush of the buffer + fsync. No-op in `:sync` mode.
  Useful for callers that want to establish a durability barrier
  between phases of a batch operation.

  `timeout` bounds the GenServer call (default 5s). Callers on
  money-commit paths should pass their commit budget explicitly —
  a stalled fsync must surface at the caller's own boundary, not
  as a surprise 5s exit mid-commit.

  Note that `read_all/2` / `read_all_strict/2` replay from disk
  only: in `:buffered` mode an accepted entry is invisible to reads
  until a flush (periodic or explicit) lands it. A reader that must
  observe everything written so far flushes first.
  """
  @spec flush(GenServer.server(), timeout()) :: :ok | {:error, term()}
  def flush(server \\ __MODULE__, timeout \\ 5_000) do
    GenServer.call(server, :flush, timeout)
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

  @doc """
  Strict variant of `read_all/1`. Returns `{:error, {:corrupt_entries,
  count}}` when any WAL segment contains CRC-bad entries — the lenient
  `read_all/1` variant silently drops them. Use from boot-replay paths
  where proceeding with less history than the leader wrote is unsafe.
  """
  @spec read_all_strict(GenServer.server(), timeout()) ::
          {:ok, [Entry.t()]} | {:error, term()}
  def read_all_strict(server \\ __MODULE__, timeout \\ 30_000) do
    GenServer.call(server, :read_all_strict, timeout)
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

  @doc """
  Clears all WAL data while the server is running: closes the current
  segment, deletes every segment file, and reopens a fresh segment 1.

  Unlike `truncate/1` — a directory-level operation that is unsafe
  under a live server (the process keeps writing to the unlinked
  file) — this coordinates with the GenServer, so subsequent appends
  land in the fresh segment and later reads see an empty WAL.
  """
  @spec clear(GenServer.server()) :: :ok | {:error, term()}
  def clear(server \\ __MODULE__) do
    GenServer.call(server, :clear)
  end

  # --- GenServer Callbacks ---

  @impl GenServer
  def init(opts) do
    Process.flag(:trap_exit, true)

    wal_dir = Keyword.fetch!(opts, :wal_dir)
    validate_path!(wal_dir)

    type_map = Keyword.fetch!(opts, :types)
    max_segment = Keyword.get(opts, :max_segment_bytes, @default_max_segment_bytes)
    mode = Keyword.get(opts, :mode, :sync)
    validate_mode!(mode)

    flush_interval_ms = Keyword.get(opts, :flush_interval_ms, @default_flush_interval_ms)
    flush_max_entries = Keyword.get(opts, :flush_max_entries, @default_flush_max_entries)

    validate_type_map!(type_map)
    byte_map = Map.new(type_map, fn {k, v} -> {v, k} end)
    File.mkdir_p!(wal_dir)

    :ok = halt_on_corrupt_segments!(wal_dir)

    {segment_index, entry_count, bytes_written} = scan_existing_segments(wal_dir, byte_map)
    segment_path = segment_path(wal_dir, segment_index)

    case open_or_create_segment(segment_path) do
      {:ok, fd, actual_bytes} ->
        state = %{
          wal_dir: wal_dir,
          fd: fd,
          segment_index: segment_index,
          segment_path: segment_path,
          entry_count: entry_count,
          bytes_written: max(bytes_written, actual_bytes),
          writes_halted: false,
          type_map: type_map,
          byte_map: byte_map,
          max_segment_bytes: max_segment,
          mode: mode,
          flush_interval_ms: flush_interval_ms,
          flush_max_entries: flush_max_entries,
          # Buffered mode: encoded entry binaries in reverse order
          # (cheap prepend, reversed on flush). Empty in :sync mode.
          buffer: [],
          buffer_count: 0,
          buffer_bytes: 0
        }

        if mode == :buffered, do: schedule_periodic_flush(flush_interval_ms)

        {:ok, state}

      {:error, reason} ->
        {:stop, {:wal_open_failed, reason}}
    end
  end

  @impl GenServer
  def handle_call({:append, _entry}, _from, %{writes_halted: true} = state) do
    {:reply, {:error, :writes_halted}, state}
  end

  def handle_call({:append, entry}, _from, %{mode: :sync} = state) do
    reply_after_sync_write(entry, state)
  end

  def handle_call({:append, entry}, _from, %{mode: :buffered} = state) do
    reply_after_buffered_write(entry, state)
  end

  def handle_call({:append_sync, _entry}, _from, %{writes_halted: true} = state) do
    {:reply, {:error, :writes_halted}, state}
  end

  def handle_call({:append_sync, entry}, _from, %{mode: :sync} = state) do
    reply_after_sync_write(entry, state)
  end

  def handle_call({:append_sync, entry}, _from, %{mode: :buffered} = state) do
    # Drain the buffer first so the caller's guarantee is total
    # ordering: every entry accepted before this one is durable too.
    case do_flush(state) do
      {:ok, flushed_state} -> reply_after_sync_write(entry, flushed_state)
      {:stop, reason, halted_state} -> {:stop, reason, {:error, reason}, halted_state}
    end
  end

  def handle_call({:append_batch, _entries}, _from, %{writes_halted: true} = state) do
    {:reply, {:error, :writes_halted}, state}
  end

  def handle_call({:append_batch, entries}, _from, %{mode: :sync} = state) do
    reply_after_sync_batch_write(entries, state)
  end

  def handle_call({:append_batch, entries}, _from, %{mode: :buffered} = state) do
    reply_after_buffered_batch_write(entries, state)
  end

  def handle_call(:flush, _from, %{mode: :sync} = state), do: {:reply, :ok, state}

  def handle_call(:flush, _from, %{writes_halted: true} = state) do
    {:reply, {:error, :writes_halted}, state}
  end

  def handle_call(:flush, _from, %{mode: :buffered} = state) do
    case do_flush(state) do
      {:ok, flushed_state} -> {:reply, :ok, flushed_state}
      {:stop, reason, halted_state} -> {:stop, reason, {:error, reason}, halted_state}
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

  def handle_call(:read_all_strict, _from, state) do
    case replay(state.wal_dir, state.byte_map, skip_corrupt: true) do
      {:ok, entries, 0} ->
        {:reply, {:ok, entries}, state}

      {:ok, _entries, corrupt_count} when corrupt_count > 0 ->
        # CRC-bad entries were silently dropped by `read_all/1` for the
        # convenience of tools/introspection. `read_all_strict/1`
        # surfaces the corruption so a boot-time caller can fail closed
        # instead of proceeding with less history — a bit-rotted or
        # prematurely-pruned leader segment produced a stream with a
        # silent hole that followers used to skip over forever.
        {:reply, {:error, {:corrupt_entries, corrupt_count}}, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call(:checkpoint, _from, %{writes_halted: true} = state) do
    {:reply, {:error, :writes_halted}, state}
  end

  def handle_call(:checkpoint, _from, state) do
    # Checkpoint always drains any pending buffered writes first so
    # the sealed count reflects everything the WAL has accepted.
    case maybe_drain_buffer(state) do
      {:ok, drained_state} -> do_checkpoint(drained_state)
      {:stop, reason, halted_state} -> {:stop, reason, {:error, reason}, halted_state}
    end
  end

  def handle_call(:rotate, _from, state) do
    case maybe_drain_buffer(state) do
      {:ok, drained_state} ->
        case do_rotate(drained_state) do
          {:ok, new_state} -> {:reply, :ok, new_state}
          {:error, reason} -> {:reply, {:error, reason}, drained_state}
        end

      {:stop, reason, halted_state} ->
        {:stop, reason, {:error, reason}, halted_state}
    end
  end

  def handle_call(:clear, _from, state) do
    new_path = segment_path(state.wal_dir, 1)

    with :ok <- safe_close_fd(state.fd),
         :ok <- truncate(state.wal_dir),
         {:ok, fd, bytes} <- open_or_create_segment(new_path) do
      {:reply, :ok,
       %{
         state
         | fd: fd,
           segment_index: 1,
           segment_path: new_path,
           entry_count: 0,
           bytes_written: bytes,
           writes_halted: false,
           buffer: [],
           buffer_count: 0,
           buffer_bytes: 0
       }}
    else
      {:error, reason} -> {:reply, {:error, {:wal_clear_failed, reason}}, state}
    end
  end

  def handle_call(:info, _from, state) do
    {:reply,
     %{
       wal_dir: state.wal_dir,
       segment_index: state.segment_index,
       segment_path: state.segment_path,
       entry_count: state.entry_count,
       bytes_written: state.bytes_written,
       mode: state.mode,
       buffer_count: state.buffer_count,
       buffer_bytes: state.buffer_bytes
     }, state}
  end

  @impl GenServer
  def handle_info(:periodic_flush, %{mode: :buffered} = state) do
    case do_flush(state) do
      {:ok, flushed_state} ->
        schedule_periodic_flush(flushed_state.flush_interval_ms)
        {:noreply, flushed_state}

      {:stop, reason, halted_state} ->
        {:stop, reason, halted_state}
    end
  end

  def handle_info(:periodic_flush, state), do: {:noreply, state}
  def handle_info(_msg, state), do: {:noreply, state}

  @impl GenServer
  def terminate(_reason, state) do
    if state[:fd] do
      try do
        # Persist final entry_count into the segment header on
        # graceful shutdown so a subsequent read that keys off the
        # header (rather than scanning to EOF) sees the exact count.
        # Not required for correctness — read paths handle a stale
        # header — but keeps the segment's on-disk metadata clean
        # after a normal stop.
        flushed_state =
          case do_flush(state) do
            {:ok, s} -> s
            {:stop, _reason, s} -> s
          end

        _refresh_result = update_header_count(flushed_state)
        :file.sync(flushed_state.fd)
        :file.close(flushed_state.fd)
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

  defp validate_mode!(:sync), do: :ok
  defp validate_mode!(:buffered), do: :ok

  defp validate_mode!(other),
    do: raise(ArgumentError, "WAL :mode must be :sync or :buffered (got: #{inspect(other)})")

  defp schedule_periodic_flush(interval_ms) do
    Process.send_after(self(), :periodic_flush, interval_ms)
  end

  # Sync-write path: write + fsync + rotate check + reply :ok.
  # Shared by :sync mode's append and :buffered mode's append_sync.
  defp reply_after_sync_write(entry, state) do
    bin = Entry.encode(entry, state.type_map)

    with :ok <- :file.write(state.fd, bin),
         :ok <- :file.sync(state.fd) do
      new_state = %{
        state
        | bytes_written: state.bytes_written + byte_size(bin),
          entry_count: state.entry_count + 1
      }

      # Per-append header count update dropped: it was a second
      # fsync per append (seek + write + sync + seek), doubling
      # latency. The header count is authoritative only in the
      # "no rotation, clean shutdown" case; the read path
      # (`read_segment_entries` + `Entry.decode_all`) already
      # tolerates a stale header count by scanning to EOF, so a
      # per-append header write buys nothing that the entry stream
      # itself doesn't already deliver. Header count is refreshed
      # on segment rotation (below) and on graceful shutdown
      # (`terminate/2`).
      new_state = maybe_rotate(new_state)
      {:reply, :ok, new_state}
    else
      {:error, reason} ->
        Logger.error("WAL: write/fsync failed: #{inspect(reason)}")
        {:reply, {:error, {:wal_write_failed, reason}}, state}
    end
  end

  # Buffered-write path: enqueue + return :ok. Auto-flushes when
  # the buffer hits the entry-count ceiling.
  defp reply_after_buffered_write(entry, state) do
    bin = Entry.encode(entry, state.type_map)
    bin_size = byte_size(bin)

    new_state = %{
      state
      | buffer: [bin | state.buffer],
        buffer_count: state.buffer_count + 1,
        buffer_bytes: state.buffer_bytes + bin_size
    }

    if new_state.buffer_count >= new_state.flush_max_entries do
      case do_flush(new_state) do
        {:ok, flushed_state} -> {:reply, :ok, flushed_state}
        {:stop, reason, halted_state} -> {:stop, reason, {:error, reason}, halted_state}
      end
    else
      {:reply, :ok, new_state}
    end
  end

  # Atomic batch path for :sync mode: encode ALL entries first
  # (an encode failure aborts before anything is written), then a
  # single write + fsync for the whole batch. The batch either
  # lands in full or not at all — no durable prefix can strand
  # behind a mid-batch per-entry failure.
  defp reply_after_sync_batch_write(entries, state) do
    with {:ok, bins} <- encode_all(entries, state.type_map),
         :ok <- :file.write(state.fd, bins),
         :ok <- :file.sync(state.fd) do
      new_state = %{
        state
        | bytes_written: state.bytes_written + IO.iodata_length(bins),
          entry_count: state.entry_count + length(entries)
      }

      new_state = maybe_rotate(new_state)
      {:reply, :ok, new_state}
    else
      {:error, {:encode_failed, _reason}} = err ->
        {:reply, err, state}

      {:error, reason} ->
        Logger.error("WAL: batch write/fsync failed: #{inspect(reason)}")
        {:reply, {:error, {:wal_write_failed, reason}}, state}
    end
  end

  # Atomic batch path for :buffered mode: encode everything upfront
  # (aborting cleanly on a bad entry), then enqueue the batch as one
  # group. The buffer flushes as a single write, so the batch can
  # never be split across flushes by per-entry arrivals from other
  # callers — this handler runs inside the GenServer, so no flush
  # can interleave mid-enqueue.
  defp reply_after_buffered_batch_write(entries, state) do
    case encode_all(entries, state.type_map) do
      {:ok, bins} ->
        batch_bytes = IO.iodata_length(bins)

        new_state = %{
          state
          | buffer: Enum.reverse(bins) ++ state.buffer,
            buffer_count: state.buffer_count + length(entries),
            buffer_bytes: state.buffer_bytes + batch_bytes
        }

        if new_state.buffer_count >= new_state.flush_max_entries do
          case do_flush(new_state) do
            {:ok, flushed_state} -> {:reply, :ok, flushed_state}
            {:stop, reason, halted_state} -> {:stop, reason, {:error, reason}, halted_state}
          end
        else
          {:reply, :ok, new_state}
        end

      {:error, _reason} = err ->
        {:reply, err, state}
    end
  end

  # Encodes every entry or none: the first encode failure returns
  # `{:error, {:encode_failed, reason}}` with no partial result so
  # batch callers abort before any write reaches the segment.
  defp encode_all(entries, type_map) do
    {:ok, Enum.map(entries, &Entry.encode(&1, type_map))}
  rescue
    e -> {:error, {:encode_failed, Exception.message(e)}}
  end

  # Drain the buffer with one write + one fsync. Returns
  # `{:ok, state'}` on success or `{:stop, reason, state'}` on
  # failure — buffered-mode flush failures are fail-hard because
  # entries the caller was told :ok on can never be recovered.
  defp do_flush(%{buffer: []} = state), do: {:ok, state}

  defp do_flush(state) do
    # Reverse to write-order (buffer is prepended for cheap enqueue).
    entries = Enum.reverse(state.buffer)
    flushed_count = state.buffer_count
    flushed_bytes = state.buffer_bytes

    with :ok <- :file.write(state.fd, entries),
         :ok <- :file.sync(state.fd) do
      new_state = %{
        state
        | buffer: [],
          buffer_count: 0,
          buffer_bytes: 0,
          bytes_written: state.bytes_written + flushed_bytes,
          entry_count: state.entry_count + flushed_count
      }

      {:ok, maybe_rotate(new_state)}
    else
      {:error, reason} ->
        Logger.error(
          "WAL: buffered flush of #{flushed_count} entries failed: #{inspect(reason)} — halting"
        )

        halted_state = %{state | writes_halted: true}
        {:stop, {:flush_failed, reason}, halted_state}
    end
  end

  # Used by paths that need to reason about a clean on-disk state
  # (checkpoint, rotate) — flushes pending buffer if any.
  defp maybe_drain_buffer(%{mode: :sync} = state), do: {:ok, state}
  defp maybe_drain_buffer(%{buffer: []} = state), do: {:ok, state}
  defp maybe_drain_buffer(state), do: do_flush(state)

  defp do_checkpoint(state) do
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

  defp safe_close_fd(fd) do
    :file.close(fd)
  catch
    _kind, _reason -> :ok
  end

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
            quarantine_corrupt_segment(path)
            create_new_segment(path)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  # A segment whose header is unreadable (torn write, bitrot) is
  # quarantined — renamed aside with a .corrupt suffix — rather than
  # truncated in place. The entry bytes after the bad header may be
  # recoverable forensically, and the .corrupt file is the signal
  # recovery tooling uses to refuse a silent boot. In-place truncation
  # destroyed exactly the most recent entries, because the WAL process
  # opens the active segment before any recovery pass can read it.
  defp quarantine_corrupt_segment(path) do
    corrupt_path = path <> ".corrupt"

    case File.rename(path, corrupt_path) do
      :ok ->
        Logger.error(
          "WAL: segment #{path} has an unreadable header — quarantined to #{corrupt_path}; " <>
            "entry bytes preserved for forensics"
        )

      {:error, reason} ->
        Logger.error(
          "WAL: could not quarantine corrupt segment #{path}: #{inspect(reason)} — " <>
            "falling back to truncate on re-create"
        )
    end
  end

  defp create_new_segment(path) do
    header = <<@magic, @version::unsigned-big-16, 0::unsigned-big-32>>

    # Header-corrupt segments reach here only after
    # quarantine_corrupt_segment/1 has renamed them aside, so the
    # truncate below is a fallback for a failed rename (and a no-op
    # for genuinely new files). (:truncate in the open modes is
    # silently ignored when :read is present, hence the explicit
    # :file.truncate/1.)
    case :file.open(String.to_charlist(path), [:raw, :binary, :read, :write]) do
      {:ok, fd} ->
        :ok = :file.truncate(fd)

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

  # Header-corrupt segments are renamed aside with a `.corrupt` suffix
  # (see `quarantine_corrupt_segment/1`) so entry bytes stay preserved
  # for forensic recovery. Their presence means a prior boot detected
  # bit-level damage in the sealed history and silently continued —
  # money-state may already have re-materialised wrong. Refuse to open
  # the WAL until an operator has reviewed and either restored the
  # segment or deliberately cleared the marker.
  defp halt_on_corrupt_segments!(wal_dir) do
    case File.ls(wal_dir) do
      {:ok, files} ->
        corrupt =
          files
          |> Enum.filter(&Regex.match?(~r/^wal\.\d+\.corrupt$/, &1))
          |> Enum.sort()

        if corrupt == [] do
          :ok
        else
          full = Enum.map(corrupt, &Path.join(wal_dir, &1))

          raise """
          Locker.WAL: found quarantined `.corrupt` segments in #{wal_dir} \
          (#{Enum.join(full, ", ")}). A prior boot detected a header-\
          corrupt segment and renamed it aside while creating a fresh \
          segment at the same index — sealed entries in the quarantined \
          file never re-materialised. Refusing to start until the segments \
          are reviewed. Investigate the entry bytes for money-state impact, \
          then either restore the segment or (deliberately) remove the \
          `.corrupt` file to acknowledge the loss.
          """
        end

      {:error, _reason} ->
        :ok
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
