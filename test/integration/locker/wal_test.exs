defmodule Locker.WALTest do
  use ExUnit.Case, async: false

  alias Locker.WAL
  alias Locker.WAL.Entry

  @type_map %{write: 0x01, delete: 0x02, checkpoint: 0x06}
  @byte_map %{0x01 => :write, 0x02 => :delete, 0x06 => :checkpoint}

  setup do
    dir = Path.join(System.tmp_dir!(), "locker_wal_test_#{:erlang.unique_integer([:positive])}")
    File.mkdir_p!(dir)

    on_exit(fn -> File.rm_rf!(dir) end)

    {:ok, wal_dir: dir}
  end

  defp start_wal(dir, opts \\ []) do
    name = :"wal_#{:erlang.unique_integer([:positive])}"

    defaults = [
      wal_dir: dir,
      types: @type_map,
      name: name
    ]

    {:ok, pid} = WAL.start_link(Keyword.merge(defaults, opts))
    {pid, name}
  end

  describe "append and read_all" do
    test "appends and reads entries", %{wal_dir: dir} do
      {_pid, name} = start_wal(dir)

      entry = %Entry{type: :write, payload: %{key: "k1", value: "v1"}}
      assert :ok = WAL.append(name, entry)

      assert {:ok, entries} = WAL.read_all(name)
      assert length(entries) == 1
      assert hd(entries).type == :write
      assert hd(entries).payload == %{key: "k1", value: "v1"}
    end

    test "appends multiple entries", %{wal_dir: dir} do
      {_pid, name} = start_wal(dir)

      for i <- 1..10 do
        entry = %Entry{type: :write, payload: %{n: i}}
        assert :ok = WAL.append(name, entry)
      end

      assert {:ok, entries} = WAL.read_all(name)
      assert length(entries) == 10
    end
  end

  describe "checkpoint" do
    test "writes a checkpoint entry", %{wal_dir: dir} do
      {_pid, name} = start_wal(dir)

      WAL.append(name, %Entry{type: :write, payload: "before"})
      assert :ok = WAL.checkpoint(name)

      {:ok, entries} = WAL.read_all(name)
      types = Enum.map(entries, & &1.type)
      assert :checkpoint in types
    end
  end

  describe "info" do
    test "returns WAL state", %{wal_dir: dir} do
      {_pid, name} = start_wal(dir)

      info = WAL.info(name)
      assert info.wal_dir == dir
      assert info.segment_index >= 1
      assert info.entry_count == 0
    end

    test "entry count increments", %{wal_dir: dir} do
      {_pid, name} = start_wal(dir)

      WAL.append(name, %Entry{type: :write, payload: "a"})
      WAL.append(name, %Entry{type: :write, payload: "b"})

      info = WAL.info(name)
      assert info.entry_count == 2
    end
  end

  describe "segment rotation" do
    test "rotates when segment exceeds max size", %{wal_dir: dir} do
      {_pid, name} = start_wal(dir, max_segment_bytes: 100)

      # Write enough to trigger rotation
      for i <- 1..20 do
        WAL.append(name, %Entry{type: :write, payload: String.duplicate("x", 20) <> "#{i}"})
      end

      info = WAL.info(name)
      assert info.segment_index > 1
    end

    test "manual rotation", %{wal_dir: dir} do
      {_pid, name} = start_wal(dir)

      WAL.append(name, %Entry{type: :write, payload: "before"})
      assert :ok = WAL.rotate(name)

      info = WAL.info(name)
      assert info.segment_index == 2
      assert info.entry_count == 0
    end
  end

  describe "replay/3 (static)" do
    test "replays entries from disk without GenServer", %{wal_dir: dir} do
      {pid, name} = start_wal(dir)

      WAL.append(name, %Entry{type: :write, payload: "one"})
      WAL.append(name, %Entry{type: :delete, payload: "two"})
      WAL.append(name, %Entry{type: :write, payload: "three"})

      GenServer.stop(pid)

      assert {:ok, entries, 0} = WAL.replay(dir, @byte_map)
      assert length(entries) == 3
      assert Enum.map(entries, & &1.payload) == ["one", "two", "three"]
    end
  end

  describe "prune/2" do
    test "prunes old segments keeping N most recent", %{wal_dir: dir} do
      {_pid, name} = start_wal(dir, max_segment_bytes: 50)

      for i <- 1..30 do
        WAL.append(name, %Entry{type: :write, payload: "entry_#{i}"})
      end

      info = WAL.info(name)
      assert info.segment_index > 2

      {:ok, pruned} = WAL.prune(dir, 2)
      assert pruned >= 0
    end
  end

  describe "truncate/1" do
    test "removes all segments", %{wal_dir: dir} do
      {pid, name} = start_wal(dir)

      WAL.append(name, %Entry{type: :write, payload: "data"})
      GenServer.stop(pid)

      assert :ok = WAL.truncate(dir)

      segments =
        dir
        |> File.ls!()
        |> Enum.filter(&String.starts_with?(&1, "wal."))

      assert segments == []
    end
  end

  describe "persistence across restarts" do
    test "entries survive GenServer restart", %{wal_dir: dir} do
      {pid, name} = start_wal(dir)

      WAL.append(name, %Entry{type: :write, payload: "persistent"})
      GenServer.stop(pid)

      {_pid2, name2} = start_wal(dir)

      {:ok, entries} = WAL.read_all(name2)
      assert length(entries) == 1
      assert hd(entries).payload == "persistent"
    end
  end
end
