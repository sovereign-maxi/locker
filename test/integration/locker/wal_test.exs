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

  describe "segment re-creation after header corruption" do
    test "first-detection boot halts loudly AND quarantines the corrupt segment", %{wal_dir: dir} do
      {pid, name} = start_wal(dir)

      :ok =
        WAL.append(name, %Entry{
          type: :checkpoint,
          payload: %{"blob" => String.duplicate("a", 500)}
        })

      GenServer.stop(pid)

      # Tear the header: zero the first 10 bytes, leave entry bytes intact
      segment = Path.join(dir, "wal.000001")
      {:ok, fd} = :file.open(String.to_charlist(segment), [:raw, :binary, :read, :write])
      :ok = :file.pwrite(fd, 0, :binary.copy(<<0>>, 10))
      :file.close(fd)

      # First-detection boot MUST halt — torn writes can't produce a
      # corrupt header (entry CRC covers torn tails), so a bad header
      # indicates bitrot / backup corruption / tampering, and silently
      # quarantining-and-continuing would drop sealed history for a
      # boot before the next boot's halt-on-corrupt check fires.
      Process.flag(:trap_exit, true)

      assert {:error, {%RuntimeError{message: msg}, _stack}} =
               WAL.start_link(
                 name: :"locker_test_wal_first_detect_#{:erlang.unique_integer([:positive])}",
                 wal_dir: dir,
                 types: %{checkpoint: 0x01, write: 0x02, delete: 0x03}
               )

      assert msg =~ "header-corrupt segment"

      Process.flag(:trap_exit, false)

      # The corrupt original survives alongside with its entry bytes intact —
      # quarantine ran before the halt so forensic recovery still has the
      # sealed entries.
      corrupt = segment <> ".corrupt"
      assert File.exists?(corrupt)
      {:ok, bytes} = File.read(corrupt)
      assert byte_size(bytes) > 500
      assert :binary.matches(bytes, String.duplicate("a", 500)) != []
    end

    test "subsequent boot with a `.corrupt` segment on disk refuses to start", %{wal_dir: dir} do
      {pid, name} = start_wal(dir)
      :ok = WAL.append(name, %Entry{type: :checkpoint, payload: %{"blob" => "seeded"}})
      GenServer.stop(pid)

      # Simulate a prior boot's quarantine result.
      corrupt_path = Path.join(dir, "wal.000001.corrupt")
      File.rename!(Path.join(dir, "wal.000001"), corrupt_path)

      # Locker.WAL.start_link raises via init's `halt_on_corrupt_segments!`;
      # GenServer converts the raise into `{:error, {reason, stacktrace}}`.
      # Trap exits so the linked-process death doesn't take the test out.
      Process.flag(:trap_exit, true)

      assert {:error, {%RuntimeError{message: msg}, _stack}} =
               WAL.start_link(
                 name: :"locker_test_wal_halt_#{:erlang.unique_integer([:positive])}",
                 wal_dir: dir,
                 types: %{checkpoint: 0x01, write: 0x02, delete: 0x03}
               )

      assert msg =~ "`.corrupt` segments"

      Process.flag(:trap_exit, false)

      # Operator acknowledgement (remove the `.corrupt` marker) unblocks
      # boot — the sealed entries stay preserved as long as the marker
      # is renamed to a non-`.corrupt` name or copied elsewhere first.
      File.rm!(corrupt_path)

      {pid2, _name2} = start_wal(dir)
      GenServer.stop(pid2)
    end
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

  describe "writer_status/1" do
    test "reports ok for a healthy writer and writes_halted for a latched writer", %{wal_dir: dir} do
      {_pid, name} = start_wal(dir)
      assert WAL.writer_status(name) == :ok

      assert %{writes_halted: true} = :sys.replace_state(name, &Map.put(&1, :writes_halted, true))
      assert WAL.writer_status(name) == :writes_halted
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

  describe "clear/1" do
    test "empties a live server; subsequent appends land in a fresh segment", %{wal_dir: dir} do
      {_pid, name} = start_wal(dir)

      :ok = WAL.append(name, %Entry{type: :write, payload: "before-clear"})
      assert {:ok, [_entry]} = WAL.read_all(name)

      assert :ok = WAL.clear(name)
      assert {:ok, []} = WAL.read_all(name)

      # The server keeps working: new appends persist and replay sees
      # only the post-clear generation.
      :ok = WAL.append(name, %Entry{type: :write, payload: "after-clear"})
      assert {:ok, [entry]} = WAL.read_all(name)
      assert entry.payload == "after-clear"

      info = WAL.info(name)
      assert info.segment_index == 1
      assert info.entry_count == 1
    end

    test "a second server booted on the cleared dir sees an empty WAL", %{wal_dir: dir} do
      {pid, name} = start_wal(dir)

      :ok = WAL.append(name, %Entry{type: :write, payload: "doomed"})
      assert :ok = WAL.clear(name)
      GenServer.stop(pid)

      {_pid2, name2} = start_wal(dir)
      assert {:ok, []} = WAL.read_all(name2)
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

  describe "append latency (per-append fsync only)" do
    # The per-append handle_call used to fsync the entry write AND
    # then rewrite + fsync the segment header count on every append —
    # two fsyncs per append. Since `read_segment_entries` /
    # `Entry.decode_all` already tolerates a stale header count by
    # scanning to EOF, the header rewrite was pure latency overhead.
    # This test asserts that a large append burst produces valid
    # entries whose count is accurately recoverable from the entry
    # stream — the correctness guarantee that lets us drop the
    # per-append header fsync.
    test "large burst of appends: entry stream is authoritative for count", %{wal_dir: dir} do
      {pid, name} = start_wal(dir)

      burst = 500

      {us, :ok} =
        :timer.tc(fn ->
          for i <- 1..burst do
            :ok = WAL.append(name, %Entry{type: :write, payload: "burst-#{i}"})
          end

          :ok
        end)

      per_us = Float.round(us / burst, 2)

      IO.puts("Locker.WAL.append/2 burst: #{burst} appends in #{us}μs (#{per_us}μs/append)")

      # Kill GenServer HARD so terminate/2's header-refresh doesn't
      # run: we want the read path to prove itself against a stale
      # header (or a header the crash-safe branch left at zero).
      # Unlink first — start_link/1 links the WAL to this test proc.
      Process.unlink(pid)
      ref = Process.monitor(pid)
      Process.exit(pid, :kill)

      assert_receive {:DOWN, ^ref, :process, ^pid, _reason}, 5_000

      # Boot a fresh GenServer against the same dir. It scans the
      # last segment and reports entry_count from the entry stream.
      {_pid2, name2} = start_wal(dir)

      info = WAL.info(name2)
      assert info.entry_count == burst

      # And read_all returns exactly the same set.
      {:ok, entries} = WAL.read_all(name2)
      assert length(entries) == burst
      assert Enum.map(entries, & &1.payload) == Enum.map(1..burst, &"burst-#{&1}")
    end

    test "entry-stream count matches Entry.decode_all output regardless of header count",
         %{wal_dir: dir} do
      {pid, name} = start_wal(dir)

      for i <- 1..50 do
        :ok = WAL.append(name, %Entry{type: :write, payload: "e-#{i}"})
      end

      # Hard-kill (no terminate/2 header refresh). Unlink first —
      # start_link linked WAL to this test process.
      Process.unlink(pid)
      ref = Process.monitor(pid)
      Process.exit(pid, :kill)
      assert_receive {:DOWN, ^ref, :process, ^pid, _reason}, 5_000

      {:ok, entries, 0} = WAL.replay(dir, @byte_map)
      assert length(entries) == 50
    end
  end

  describe "buffered mode" do
    test "append returns :ok immediately; entries not on disk until flush", %{wal_dir: dir} do
      {_pid, name} =
        start_wal(dir, mode: :buffered, flush_max_entries: 1_000, flush_interval_ms: 60_000)

      for i <- 1..10 do
        :ok = WAL.append(name, %Entry{type: :write, payload: "e-#{i}"})
      end

      # Nothing flushed yet — on-disk read sees zero entries.
      {:ok, on_disk, 0} = WAL.replay(dir, @byte_map)
      assert on_disk == []

      info = WAL.info(name)
      assert info.mode == :buffered
      assert info.buffer_count == 10
    end

    test "flush drains buffer to disk with one fsync", %{wal_dir: dir} do
      {_pid, name} =
        start_wal(dir, mode: :buffered, flush_max_entries: 1_000, flush_interval_ms: 60_000)

      for i <- 1..25 do
        :ok = WAL.append(name, %Entry{type: :write, payload: "e-#{i}"})
      end

      :ok = WAL.flush(name)

      {:ok, entries, 0} = WAL.replay(dir, @byte_map)
      assert length(entries) == 25
      assert Enum.map(entries, & &1.payload) == Enum.map(1..25, &"e-#{&1}")

      info = WAL.info(name)
      assert info.buffer_count == 0
    end

    test "auto-flush triggers when flush_max_entries reached", %{wal_dir: dir} do
      {_pid, name} =
        start_wal(dir, mode: :buffered, flush_max_entries: 5, flush_interval_ms: 60_000)

      for i <- 1..12 do
        :ok = WAL.append(name, %Entry{type: :write, payload: "e-#{i}"})
      end

      # 5 + 5 = 10 auto-flushed; 2 remain buffered.
      info = WAL.info(name)
      assert info.buffer_count == 2

      {:ok, entries, 0} = WAL.replay(dir, @byte_map)
      assert length(entries) == 10
    end

    test "append_sync drains buffer + writes given entry durably", %{wal_dir: dir} do
      {_pid, name} =
        start_wal(dir, mode: :buffered, flush_max_entries: 1_000, flush_interval_ms: 60_000)

      for i <- 1..8 do
        :ok = WAL.append(name, %Entry{type: :write, payload: "e-#{i}"})
      end

      :ok = WAL.append_sync(name, %Entry{type: :write, payload: "sync-9"})

      {:ok, entries, 0} = WAL.replay(dir, @byte_map)
      assert length(entries) == 9
      assert List.last(entries).payload == "sync-9"

      info = WAL.info(name)
      assert info.buffer_count == 0
    end

    test "flush is a no-op in :sync mode", %{wal_dir: dir} do
      {_pid, name} = start_wal(dir, mode: :sync)
      assert :ok = WAL.flush(name)
    end

    test "append_sync in :sync mode behaves like append", %{wal_dir: dir} do
      {_pid, name} = start_wal(dir, mode: :sync)

      :ok = WAL.append_sync(name, %Entry{type: :write, payload: "x"})

      {:ok, [entry], 0} = WAL.replay(dir, @byte_map)
      assert entry.payload == "x"
    end

    test "periodic flush drains buffer on its own schedule", %{wal_dir: dir} do
      {_pid, name} =
        start_wal(dir, mode: :buffered, flush_max_entries: 1_000, flush_interval_ms: 50)

      for i <- 1..4 do
        :ok = WAL.append(name, %Entry{type: :write, payload: "e-#{i}"})
      end

      Process.sleep(100)

      {:ok, entries, 0} = WAL.replay(dir, @byte_map)
      assert length(entries) == 4
    end

    test "terminate flushes buffer before closing", %{wal_dir: dir} do
      {pid, name} =
        start_wal(dir, mode: :buffered, flush_max_entries: 1_000, flush_interval_ms: 60_000)

      for i <- 1..7 do
        :ok = WAL.append(name, %Entry{type: :write, payload: "e-#{i}"})
      end

      Process.unlink(pid)
      GenServer.stop(name, :normal, 5_000)

      {:ok, entries, 0} = WAL.replay(dir, @byte_map)
      assert length(entries) == 7
    end

    test "invalid mode raises at init", %{wal_dir: dir} do
      Process.flag(:trap_exit, true)

      name = :"wal_bogus_#{:erlang.unique_integer([:positive])}"

      assert {:error, {%ArgumentError{}, _stacktrace}} =
               WAL.start_link(wal_dir: dir, types: @type_map, mode: :bogus, name: name)
    end
  end

  describe "read_from/3" do
    test "walks buffered entries and rotations in bounded resumable pages", %{wal_dir: dir} do
      {_pid, name} =
        start_wal(dir,
          mode: :buffered,
          flush_interval_ms: 60_000,
          flush_max_entries: 1_000,
          max_segment_bytes: 300
        )

      for i <- 1..12 do
        :ok =
          WAL.append(name, %Entry{type: :write, payload: %{n: i, blob: String.duplicate("x", 30)}})
      end

      assert {:ok, first} = WAL.read_from(name, :start, 5)
      assert length(first.entries) == 5
      refute first.eof?

      assert {:ok, second} = WAL.read_from(name, first.cursor, 5)
      assert length(second.entries) == 5

      assert {:ok, third} = WAL.read_from(name, second.cursor, 5)
      assert length(third.entries) == 2
      refute third.eof?

      assert Enum.map(first.entries ++ second.entries ++ third.entries, & &1.payload.n) ==
               Enum.to_list(1..12)

      assert {:ok, fourth} = WAL.read_from(name, third.cursor, 5)
      assert fourth.entries == []
      assert fourth.eof?
      assert {:ok, %{entries: [], eof?: true}} = WAL.read_from(name, fourth.cursor, 5)
    end

    test "stops before the first entry beyond a sequence cutoff", %{wal_dir: dir} do
      {_pid, name} = start_wal(dir)

      for i <- 1..3 do
        :ok = WAL.append(name, %Entry{type: :write, payload: %{seq: i}})
      end

      assert {:ok, page} = WAL.read_from(name, :start, 10, 2)
      assert Enum.map(page.entries, & &1.payload.seq) == [1, 2]
      refute page.eof?

      assert {:ok, remainder} = WAL.read_from(name, page.cursor, 10)
      assert Enum.map(remainder.entries, & &1.payload.seq) == [3]
    end

    test "returns corruption without producing a page", %{wal_dir: dir} do
      {pid, name} = start_wal(dir)
      :ok = WAL.append(name, %Entry{type: :write, payload: "first"})
      :ok = WAL.append(name, %Entry{type: :write, payload: "second"})
      GenServer.stop(pid)

      path = Path.join(dir, "wal.000001")
      {:ok, <<_header::binary-10, length::unsigned-big-32, _rest::binary>>} = File.read(path)
      second_crc_offset = 10 + 8 + length + 4
      {:ok, fd} = :file.open(String.to_charlist(path), [:raw, :binary, :read, :write])
      :ok = :file.pwrite(fd, second_crc_offset, <<0>>)
      :file.close(fd)

      {_pid, restarted_name} = start_wal(dir)

      assert {:error, {:corrupt_entry, ^path, _offset, _reason}} =
               WAL.read_from(restarted_name, :start, 10)
    end
  end
end
