defmodule Locker.Store.ETS do
  @moduledoc """
  In-memory ETS storage backend. Fastest reads, no persistence.

  ## Options

  - `:name` — ETS table name (required)
  - `:access` — `:public`, `:protected`, or `:private` (default: `:public`)
  """

  @behaviour Locker.Store

  @impl Locker.Store
  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    access = Keyword.get(opts, :access, :public)
    table = :ets.new(name, [:named_table, access, :set])
    {:ok, table}
  end

  @impl Locker.Store
  def put(table, key, value) do
    :ets.insert(table, {key, value})
    :ok
  end

  @impl Locker.Store
  def put_batch(table, entries) do
    :ets.insert(table, entries)
    :ok
  end

  @impl Locker.Store
  def get(table, key) do
    case :ets.lookup(table, key) do
      [{^key, value}] -> {:ok, value}
      [] -> :not_found
    end
  rescue
    ArgumentError -> {:error, :table_not_found}
  end

  @impl Locker.Store
  def member?(table, key) do
    :ets.member(table, key)
  rescue
    ArgumentError -> false
  end

  @impl Locker.Store
  def delete(table, key) do
    :ets.delete(table, key)
    :ok
  end

  @impl Locker.Store
  def size(table) do
    :ets.info(table, :size)
  end

  @impl Locker.Store
  def to_list(table) do
    :ets.tab2list(table)
  end

  @impl Locker.Store
  def close(table) do
    :ets.delete(table)
    :ok
  rescue
    ArgumentError -> :ok
  end
end
