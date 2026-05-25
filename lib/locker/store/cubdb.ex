defmodule Locker.Store.CubDB do
  @moduledoc """
  CubDB storage backend. Pure Elixir B-tree, ACID, no size limit.

  ## Options

  * `:path`: directory for CubDB data files (required)
  * `:auto_compact`: enable auto compaction (default: true)
  """

  @behaviour Locker.Store

  @impl Locker.Store
  def init(opts) do
    path = Keyword.fetch!(opts, :path)

    if String.contains?(path, "..") do
      {:error, :path_traversal}
    else
      auto_compact = Keyword.get(opts, :auto_compact, true)
      File.mkdir_p!(path)
      CubDB.start_link(data_dir: path, auto_compact: auto_compact)
    end
  end

  @impl Locker.Store
  def put(db, key, value) do
    CubDB.put(db, key, value)
    :ok
  end

  @impl Locker.Store
  def put_batch(db, entries) do
    CubDB.put_multi(db, Map.new(entries))
    :ok
  end

  @impl Locker.Store
  def get(db, key) do
    case CubDB.fetch(db, key) do
      {:ok, value} -> {:ok, value}
      :error -> :not_found
    end
  end

  @impl Locker.Store
  def member?(db, key) do
    CubDB.has_key?(db, key)
  end

  @impl Locker.Store
  def delete(db, key) do
    CubDB.delete(db, key)
    :ok
  end

  @impl Locker.Store
  def size(db) do
    CubDB.size(db)
  end

  @impl Locker.Store
  def to_list(db) do
    db |> CubDB.select() |> Enum.to_list()
  end

  @impl Locker.Store
  def close(db) do
    GenServer.stop(db)
    :ok
  rescue
    _error -> :ok
  end
end
