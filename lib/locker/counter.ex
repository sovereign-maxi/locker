defmodule Locker.Counter do
  @moduledoc """
  Atomic counter with ETS for fast reads and DETS for durability.

  Provides lock-free increments via `:ets.update_counter/4` with periodic
  DETS sync. Survives restarts by restoring from DETS on init.

  ## Usage

      {:ok, _pid} = Locker.Counter.start_link(
        name: MyApp.LiabilityCounter,
        keys: [:total_minted, :total_burned],
        data_dir: "/data",
        dets_file: "/data/reserves/liability.dets"  # optional, overrides default
      )

      Locker.Counter.increment(MyApp.LiabilityCounter, :total_minted, 1000)
      Locker.Counter.read(MyApp.LiabilityCounter, :total_minted)  # 1000
      Locker.Counter.read_all(MyApp.LiabilityCounter)  # %{total_minted: 1000, total_burned: 0}
  """

  use GenServer

  require Logger

  @dets_sync_interval_ms 5_000

  # --- Public API ---

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc "Increments a counter by the given amount. Returns the new value or error."
  @spec increment(GenServer.server(), atom(), pos_integer()) ::
          non_neg_integer() | {:error, {:unknown_key, atom()}}
  def increment(server, key, amount) when is_integer(amount) and amount > 0 do
    GenServer.call(server, {:increment, key, amount})
  end

  @doc "Reads the current value of a counter."
  @spec read(GenServer.server(), atom()) :: non_neg_integer()
  def read(server, key) do
    GenServer.call(server, {:read, key})
  end

  @doc "Reads all counters as a map."
  @spec read_all(GenServer.server()) :: %{atom() => non_neg_integer()}
  def read_all(server) do
    GenServer.call(server, :read_all)
  end

  @doc "Verifies ETS and DETS values are in sync. Returns `:ok` or `{:drift, diff}`."
  @spec verify(GenServer.server()) :: :ok | {:drift, map()}
  def verify(server) do
    GenServer.call(server, :verify)
  end

  @doc "Resets all counters to zero."
  @spec reset(GenServer.server()) :: :ok
  def reset(server) do
    GenServer.call(server, :reset)
  end

  @doc "Restores a counter to a specific value. Used during recovery."
  @spec restore(GenServer.server(), atom(), non_neg_integer()) ::
          :ok | {:error, {:unknown_key, atom()}}
  def restore(server, key, value) when is_integer(value) and value >= 0 do
    GenServer.call(server, {:restore, key, value})
  end

  # --- GenServer Callbacks ---

  @impl GenServer
  def init(opts) do
    Process.flag(:trap_exit, true)

    name = Keyword.fetch!(opts, :name)
    keys = Keyword.fetch!(opts, :keys)
    data_dir = Keyword.get(opts, :data_dir, "data")

    if String.contains?(data_dir, "..") do
      raise ArgumentError, "data_dir must not contain path traversal (..): #{data_dir}"
    end

    sync_interval = Keyword.get(opts, :sync_interval_ms, @dets_sync_interval_ms)

    # Intentional: named ETS/DETS tables require atom names derived from the server name.
    # credo:disable-for-next-line Credo.Check.Warning.UnsafeToAtom
    ets_name = :"#{name}.ETS"
    # credo:disable-for-next-line Credo.Check.Warning.UnsafeToAtom
    dets_name = :"#{name}.DETS"

    table = :ets.new(ets_name, [:named_table, :protected, :set, read_concurrency: true])

    dets_path = Keyword.get(opts, :dets_file, Path.join(data_dir, "#{name}.dets"))
    File.mkdir_p!(Path.dirname(dets_path))
    {:ok, _dets} = :dets.open_file(dets_name, file: String.to_charlist(dets_path), type: :set)
    # Restrict DETS file permissions to owner only
    File.chmod(dets_path, 0o600)

    for key <- keys do
      if :dets.lookup(dets_name, key) == [] do
        :dets.insert(dets_name, {key, 0})
      end
    end

    :dets.sync(dets_name)

    for key <- keys do
      value = dets_lookup(dets_name, key)
      :ets.insert(table, {key, value})
    end

    schedule_sync(sync_interval)

    {:ok,
     %{
       table: table,
       ets_name: ets_name,
       dets_name: dets_name,
       keys: keys,
       sync_interval_ms: sync_interval
     }}
  end

  @impl GenServer
  def handle_call({:increment, key, amount}, _from, state) do
    if key in state.keys do
      new_val = :dets.update_counter(state.dets_name, key, amount)
      :ets.insert(state.table, {key, new_val})
      {:reply, new_val, state}
    else
      {:reply, {:error, {:unknown_key, key}}, state}
    end
  end

  def handle_call({:read, key}, _from, state) do
    value = ets_lookup(state.table, key)
    {:reply, value, state}
  end

  def handle_call(:read_all, _from, state) do
    result =
      Map.new(state.keys, fn key ->
        {key, ets_lookup(state.table, key)}
      end)

    {:reply, result, state}
  end

  def handle_call(:verify, _from, state) do
    drifts =
      Enum.reduce(state.keys, %{}, fn key, acc ->
        ets_val = ets_lookup(state.table, key)
        dets_val = dets_lookup(state.dets_name, key)

        if ets_val != dets_val do
          Map.put(acc, key, %{ets: ets_val, dets: dets_val})
        else
          acc
        end
      end)

    if map_size(drifts) == 0 do
      {:reply, :ok, state}
    else
      {:reply, {:drift, drifts}, state}
    end
  end

  def handle_call(:reset, _from, state) do
    for key <- state.keys do
      :dets.insert(state.dets_name, {key, 0})
      :ets.insert(state.table, {key, 0})
    end

    :dets.sync(state.dets_name)
    {:reply, :ok, state}
  end

  def handle_call({:restore, key, value}, _from, state) do
    if key in state.keys do
      :dets.insert(state.dets_name, {key, value})
      :dets.sync(state.dets_name)
      :ets.insert(state.table, {key, value})
      {:reply, :ok, state}
    else
      {:reply, {:error, {:unknown_key, key}}, state}
    end
  end

  @impl GenServer
  def handle_info(:sync_dets, state) do
    :dets.sync(state.dets_name)
    schedule_sync(state.sync_interval_ms)
    {:noreply, state}
  end

  def handle_info(_msg, state) do
    {:noreply, state}
  end

  @impl GenServer
  def terminate(_reason, state) do
    :dets.sync(state.dets_name)
    :dets.close(state.dets_name)
    :ok
  end

  # --- Private ---

  defp schedule_sync(interval) do
    Process.send_after(self(), :sync_dets, interval)
  end

  defp dets_lookup(dets, key) do
    case :dets.lookup(dets, key) do
      [{^key, val}] -> val
      [] -> 0
    end
  end

  defp ets_lookup(table, key) do
    case :ets.lookup(table, key) do
      [{^key, value}] -> value
      [] -> 0
    end
  rescue
    ArgumentError -> 0
  end
end
