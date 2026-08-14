defmodule Locker.Store.JSONL do
  @moduledoc """
  Append-only JSONL storage backend. Human-readable, good for audit trails.

  Each line is a JSON object with `"k"` and `"v"` fields. Reads scan the
  full file and take the last value for each key (last-write-wins).

  Not suitable for high-frequency writes or large datasets. Ideal for
  configuration, audit logs, and small state files.

  ## Options

  * `:path`: file path for the JSONL file (required)
  """

  @behaviour Locker.Store

  @impl Locker.Store
  def init(opts) do
    path = Keyword.fetch!(opts, :path)

    if String.contains?(path, "..") do
      {:error, :path_traversal}
    else
      File.mkdir_p!(Path.dirname(path))
      unless File.exists?(path), do: File.write!(path, "")
      File.chmod(path, 0o600)
      {:ok, %{path: path}}
    end
  end

  @impl Locker.Store
  def put(ref, key, value) do
    case Jason.encode(%{"k" => key, "v" => value}) do
      {:ok, json} ->
        File.write!(ref.path, json <> "\n", [:append, :sync])
        :ok

      {:error, reason} ->
        {:error, {:json_encode_failed, reason}}
    end
  end

  @impl Locker.Store
  def put_batch(_ref, []), do: :ok

  def put_batch(ref, entries) do
    results = Enum.map(entries, fn {k, v} -> Jason.encode(%{"k" => k, "v" => v}) end)

    case Enum.find(results, &match?({:error, _}, &1)) do
      {:error, reason} ->
        {:error, {:json_encode_failed, reason}}

      nil ->
        lines = Enum.map_join(results, "\n", fn {:ok, json} -> json end)
        File.write!(ref.path, lines <> "\n", [:append, :sync])
        :ok
    end
  end

  @impl Locker.Store
  def get(ref, key) do
    case load_map(ref.path) do
      map when is_map(map) ->
        case Map.fetch(map, key) do
          {:ok, value} -> {:ok, value}
          :error -> :not_found
        end
    end
  end

  @impl Locker.Store
  def member?(ref, key) do
    Map.has_key?(load_map(ref.path), key)
  end

  @impl Locker.Store
  def delete(ref, key) do
    map = ref.path |> load_map() |> Map.delete(key)
    rewrite(ref.path, map)
    :ok
  end

  @impl Locker.Store
  def size(ref) do
    map_size(load_map(ref.path))
  end

  @impl Locker.Store
  def to_list(ref) do
    ref.path |> load_map() |> Enum.to_list()
  end

  @impl Locker.Store
  def close(_ref), do: :ok

  # Maximum JSONL file size to load into memory (10 MB).
  @max_file_bytes 10 * 1024 * 1024

  # --- Private ---

  defp load_map(path) do
    case File.stat(path) do
      {:ok, %{size: size}} when size > @max_file_bytes ->
        raise RuntimeError,
              "JSONL file #{path} exceeds #{@max_file_bytes} bytes; " <>
                "call delete to compact or switch to CubDB for large datasets"

      _stat ->
        case File.read(path) do
          {:ok, content} ->
            content
            |> String.split("\n", trim: true)
            |> Enum.reduce(%{}, &parse_line/2)

          {:error, :enoent} ->
            %{}

          {:error, reason} ->
            # A read error after init must be LOUD — returning an empty
            # map here would let a later delete/rewrite wipe the file.
            raise RuntimeError, "JSONL file #{path} unreadable: #{inspect(reason)}"
        end
    end
  end

  defp parse_line(line, acc) do
    case Jason.decode(line) do
      {:ok, %{"k" => k, "v" => v}} -> Map.put(acc, k, v)
      _error -> acc
    end
  end

  defp rewrite(path, map) do
    lines = Enum.map_join(map, "\n", fn {k, v} -> Jason.encode!(%{"k" => k, "v" => v}) end)
    content = if lines == "", do: "", else: lines <> "\n"

    # Atomic write: write to temp file then rename to prevent
    # data loss if the process crashes mid-write.
    tmp_path = path <> ".tmp"
    File.write!(tmp_path, content)

    case File.rename(tmp_path, path) do
      :ok ->
        :ok

      {:error, reason} ->
        File.rm(tmp_path)

        raise File.RenameError,
          reason: reason,
          action: "rename",
          source: tmp_path,
          destination: path
    end
  end
end
