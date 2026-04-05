defmodule Locker.CounterTest do
  use ExUnit.Case, async: false

  alias Locker.Counter

  setup do
    dir =
      Path.join(System.tmp_dir!(), "locker_counter_test_#{:erlang.unique_integer([:positive])}")

    File.mkdir_p!(dir)

    on_exit(fn -> File.rm_rf!(dir) end)

    {:ok, data_dir: dir}
  end

  defp start_counter(dir, opts \\ []) do
    name = :"counter_#{:erlang.unique_integer([:positive])}"

    defaults = [
      name: name,
      keys: [:minted, :burned],
      data_dir: dir
    ]

    {:ok, pid} = Counter.start_link(Keyword.merge(defaults, opts))
    {pid, name}
  end

  describe "basic operations" do
    test "starts with zero counters", %{data_dir: dir} do
      {_pid, name} = start_counter(dir)

      assert Counter.read(name, :minted) == 0
      assert Counter.read(name, :burned) == 0
    end

    test "increments a counter", %{data_dir: dir} do
      {_pid, name} = start_counter(dir)

      assert Counter.increment(name, :minted, 1000) == 1000
      assert Counter.read(name, :minted) == 1000
    end

    test "increments accumulate", %{data_dir: dir} do
      {_pid, name} = start_counter(dir)

      Counter.increment(name, :minted, 100)
      Counter.increment(name, :minted, 200)
      Counter.increment(name, :minted, 300)

      assert Counter.read(name, :minted) == 600
    end

    test "read_all returns all counters", %{data_dir: dir} do
      {_pid, name} = start_counter(dir)

      Counter.increment(name, :minted, 500)
      Counter.increment(name, :burned, 200)

      assert Counter.read_all(name) == %{minted: 500, burned: 200}
    end

    test "read returns 0 for uninitialized key accessed via ETS", %{data_dir: dir} do
      {_pid, name} = start_counter(dir)
      # Keys not in the :keys list won't be in ETS, so read returns 0
      assert Counter.read(name, :nonexistent) == 0
    end

    test "increment returns the new value each time", %{data_dir: dir} do
      {_pid, name} = start_counter(dir)

      assert Counter.increment(name, :minted, 10) == 10
      assert Counter.increment(name, :minted, 20) == 30
      assert Counter.increment(name, :minted, 5) == 35
    end

    test "different keys are independent", %{data_dir: dir} do
      {_pid, name} = start_counter(dir)

      Counter.increment(name, :minted, 100)
      Counter.increment(name, :burned, 50)

      assert Counter.read(name, :minted) == 100
      assert Counter.read(name, :burned) == 50
    end

    test "read_all returns zeros when no increments made", %{data_dir: dir} do
      {_pid, name} = start_counter(dir)
      assert Counter.read_all(name) == %{minted: 0, burned: 0}
    end
  end

  describe "reset" do
    test "resets all counters to zero", %{data_dir: dir} do
      {_pid, name} = start_counter(dir)

      Counter.increment(name, :minted, 1000)
      Counter.increment(name, :burned, 500)

      assert :ok = Counter.reset(name)

      assert Counter.read(name, :minted) == 0
      assert Counter.read(name, :burned) == 0
    end

    test "reset followed by increment works correctly", %{data_dir: dir} do
      {_pid, name} = start_counter(dir)

      Counter.increment(name, :minted, 1000)
      Counter.reset(name)
      Counter.increment(name, :minted, 42)

      assert Counter.read(name, :minted) == 42
    end

    test "multiple resets are idempotent", %{data_dir: dir} do
      {_pid, name} = start_counter(dir)

      Counter.increment(name, :minted, 100)
      Counter.reset(name)
      Counter.reset(name)

      assert Counter.read(name, :minted) == 0
    end
  end

  describe "restore" do
    test "restores a counter to a specific value", %{data_dir: dir} do
      {_pid, name} = start_counter(dir)

      assert :ok = Counter.restore(name, :minted, 9999)
      assert Counter.read(name, :minted) == 9999
    end

    test "restore to zero", %{data_dir: dir} do
      {_pid, name} = start_counter(dir)

      Counter.increment(name, :minted, 500)
      Counter.restore(name, :minted, 0)

      assert Counter.read(name, :minted) == 0
    end

    test "restore overwrites previous value", %{data_dir: dir} do
      {_pid, name} = start_counter(dir)

      Counter.increment(name, :minted, 500)
      Counter.restore(name, :minted, 100)

      assert Counter.read(name, :minted) == 100
    end

    test "restore to large value", %{data_dir: dir} do
      {_pid, name} = start_counter(dir)

      Counter.restore(name, :minted, 999_999_999_999)
      assert Counter.read(name, :minted) == 999_999_999_999
    end

    test "increment after restore adds to restored value", %{data_dir: dir} do
      {_pid, name} = start_counter(dir)

      Counter.restore(name, :minted, 1000)
      Counter.increment(name, :minted, 50)

      assert Counter.read(name, :minted) == 1050
    end
  end

  describe "persistence" do
    test "counters survive restart", %{data_dir: dir} do
      stable_name = :"persist_test_#{:erlang.unique_integer([:positive])}"

      {:ok, pid} =
        Counter.start_link(
          name: stable_name,
          keys: [:minted, :burned],
          data_dir: dir
        )

      Counter.increment(stable_name, :minted, 42_000)
      Counter.increment(stable_name, :burned, 10_000)

      GenServer.stop(pid)

      {:ok, _pid2} =
        Counter.start_link(
          name: stable_name,
          keys: [:minted, :burned],
          data_dir: dir
        )

      assert Counter.read(stable_name, :minted) == 42_000
      assert Counter.read(stable_name, :burned) == 10_000
    end

    test "restored values persist across restart", %{data_dir: dir} do
      stable_name = :"persist_restore_#{:erlang.unique_integer([:positive])}"

      {:ok, pid} =
        Counter.start_link(
          name: stable_name,
          keys: [:minted],
          data_dir: dir
        )

      Counter.restore(stable_name, :minted, 7777)
      GenServer.stop(pid)

      {:ok, _pid2} =
        Counter.start_link(
          name: stable_name,
          keys: [:minted],
          data_dir: dir
        )

      assert Counter.read(stable_name, :minted) == 7777
    end

    test "reset persists across restart", %{data_dir: dir} do
      stable_name = :"persist_reset_#{:erlang.unique_integer([:positive])}"

      {:ok, pid} =
        Counter.start_link(
          name: stable_name,
          keys: [:minted],
          data_dir: dir
        )

      Counter.increment(stable_name, :minted, 500)
      Counter.reset(stable_name)
      GenServer.stop(pid)

      {:ok, _pid2} =
        Counter.start_link(
          name: stable_name,
          keys: [:minted],
          data_dir: dir
        )

      assert Counter.read(stable_name, :minted) == 0
    end
  end

  describe "multiple keys" do
    test "supports many keys", %{data_dir: dir} do
      keys = Enum.map(1..10, &:"key_#{&1}")
      {_pid, name} = start_counter(dir, keys: keys)

      for {key, i} <- Enum.with_index(keys, 1) do
        Counter.increment(name, key, i * 100)
      end

      all = Counter.read_all(name)
      assert map_size(all) == 10

      for {key, i} <- Enum.with_index(keys, 1) do
        assert all[key] == i * 100
      end
    end

    test "single key counter", %{data_dir: dir} do
      {_pid, name} = start_counter(dir, keys: [:only])
      Counter.increment(name, :only, 1)
      assert Counter.read_all(name) == %{only: 1}
    end
  end

  describe "concurrent operations" do
    test "concurrent increments produce correct total", %{data_dir: dir} do
      {_pid, name} = start_counter(dir)

      tasks =
        for _i <- 1..50 do
          Task.async(fn -> Counter.increment(name, :minted, 1) end)
        end

      Task.await_many(tasks)
      assert Counter.read(name, :minted) == 50
    end
  end
end
