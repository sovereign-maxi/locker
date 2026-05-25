defmodule Locker.Store.DETS do
  @moduledoc """
  File-backed DETS storage backend. Persistent, 2GB limit.

  Syncs to disk after every write by default. For high-throughput
  scenarios, consider using `Locker.Counter` (which batches syncs)
  or `Locker.Store.CubDB` (which has built-in write batching).

  ## Options

  * `:name`: DETS table name (required)
  * `:path`: file path for the DETS file (required)
  """

  @behaviour Locker.Store

  @impl Locker.Store
  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    path = Keyword.fetch!(opts, :path)

    if String.contains?(path, "..") do
      {:error, :path_traversal}
    else
      File.mkdir_p!(Path.dirname(path))
      {:ok, _dets} = :dets.open_file(name, file: String.to_charlist(path), type: :set)
      File.chmod(path, 0o600)
      {:ok, name}
    end
  end

  @impl Locker.Store
  def put(name, key, value) do
    :dets.insert(name, {key, value})
    :dets.sync(name)
    :ok
  end

  @impl Locker.Store
  def put_batch(name, entries) do
    :dets.insert(name, entries)
    :dets.sync(name)
    :ok
  end

  @impl Locker.Store
  def get(name, key) do
    case :dets.lookup(name, key) do
      [{^key, value}] -> {:ok, value}
      [] -> :not_found
    end
  end

  @impl Locker.Store
  def member?(name, key) do
    :dets.lookup(name, key) != []
  end

  @impl Locker.Store
  def delete(name, key) do
    :dets.delete(name, key)
    :dets.sync(name)
    :ok
  end

  @impl Locker.Store
  def size(name) do
    :dets.info(name, :size)
  end

  @doc """
  Returns all entries. Warning: loads entire table into memory.
  For large DETS files (near 2GB limit), consider iterating instead.
  """
  @impl Locker.Store
  def to_list(name) do
    :dets.foldl(fn entry, acc -> [entry | acc] end, [], name)
  end

  @impl Locker.Store
  def close(name) do
    :dets.sync(name)
    :dets.close(name)
    :ok
  end
end
