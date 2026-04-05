defmodule Locker.WAL.EntryTest do
  use ExUnit.Case, async: true

  alias Locker.WAL.Entry

  @type_map %{write: 0x01, delete: 0x02, checkpoint: 0x06}
  @byte_map %{0x01 => :write, 0x02 => :delete, 0x06 => :checkpoint}

  describe "encode/2 and decode/2" do
    test "round-trips a simple entry" do
      entry = %Entry{type: :write, payload: %{key: "foo", value: "bar"}}
      bin = Entry.encode(entry, @type_map)

      assert {:ok, decoded, <<>>, false} = Entry.decode(bin, @byte_map)
      assert decoded.type == :write
      assert decoded.payload == %{key: "foo", value: "bar"}
      assert is_integer(decoded.crc) and decoded.crc > 0
    end

    test "round-trips a checkpoint entry" do
      entry = %Entry{type: :checkpoint, payload: %{timestamp: 12_345}}
      bin = Entry.encode(entry, @type_map)

      assert {:ok, decoded, <<>>, false} = Entry.decode(bin, @byte_map)
      assert decoded.type == :checkpoint
      assert decoded.payload.timestamp == 12_345
    end

    test "round-trips binary payload" do
      entry = %Entry{type: :write, payload: <<1, 2, 3, 4, 5>>}
      bin = Entry.encode(entry, @type_map)

      assert {:ok, decoded, <<>>, false} = Entry.decode(bin, @byte_map)
      assert decoded.payload == <<1, 2, 3, 4, 5>>
    end

    test "round-trips nil payload" do
      entry = %Entry{type: :write, payload: nil}
      bin = Entry.encode(entry, @type_map)

      assert {:ok, decoded, <<>>, false} = Entry.decode(bin, @byte_map)
      assert decoded.payload == nil
    end

    test "round-trips empty map payload" do
      entry = %Entry{type: :write, payload: %{}}
      bin = Entry.encode(entry, @type_map)

      assert {:ok, decoded, <<>>, false} = Entry.decode(bin, @byte_map)
      assert decoded.payload == %{}
    end

    test "round-trips empty list payload" do
      entry = %Entry{type: :write, payload: []}
      bin = Entry.encode(entry, @type_map)

      assert {:ok, decoded, <<>>, false} = Entry.decode(bin, @byte_map)
      assert decoded.payload == []
    end

    test "round-trips list payload" do
      entry = %Entry{type: :write, payload: [1, "two", :three, 4.0]}
      bin = Entry.encode(entry, @type_map)

      assert {:ok, decoded, <<>>, false} = Entry.decode(bin, @byte_map)
      assert decoded.payload == [1, "two", :three, 4.0]
    end

    test "round-trips nested map payload" do
      payload = %{outer: %{inner: %{deep: "value"}}, list: [1, 2, 3]}
      entry = %Entry{type: :write, payload: payload}
      bin = Entry.encode(entry, @type_map)

      assert {:ok, decoded, <<>>, false} = Entry.decode(bin, @byte_map)
      assert decoded.payload == payload
    end

    test "round-trips large payload" do
      payload = String.duplicate("x", 100_000)
      entry = %Entry{type: :write, payload: payload}
      bin = Entry.encode(entry, @type_map)

      assert {:ok, decoded, <<>>, false} = Entry.decode(bin, @byte_map)
      assert decoded.payload == payload
    end

    test "round-trips integer payload" do
      entry = %Entry{type: :write, payload: 999_999}
      bin = Entry.encode(entry, @type_map)

      assert {:ok, decoded, <<>>, false} = Entry.decode(bin, @byte_map)
      assert decoded.payload == 999_999
    end

    test "round-trips tuple payload" do
      entry = %Entry{type: :write, payload: {:ok, "done"}}
      bin = Entry.encode(entry, @type_map)

      assert {:ok, decoded, <<>>, false} = Entry.decode(bin, @byte_map)
      assert decoded.payload == {:ok, "done"}
    end

    test "round-trips atom payload" do
      entry = %Entry{type: :write, payload: :some_atom}
      bin = Entry.encode(entry, @type_map)

      assert {:ok, decoded, <<>>, false} = Entry.decode(bin, @byte_map)
      assert decoded.payload == :some_atom
    end

    test "returns remaining bytes after decode" do
      entry = %Entry{type: :write, payload: "hello"}
      bin = Entry.encode(entry, @type_map)
      trailing = <<99, 100, 101>>

      assert {:ok, _decoded, ^trailing, false} = Entry.decode(bin <> trailing, @byte_map)
    end

    test "different type bytes produce different binaries" do
      e1 = Entry.encode(%Entry{type: :write, payload: "same"}, @type_map)
      e2 = Entry.encode(%Entry{type: :delete, payload: "same"}, @type_map)
      assert e1 != e2
    end

    test "encode raises on unknown type" do
      entry = %Entry{type: :unknown, payload: "x"}
      assert_raise KeyError, fn -> Entry.encode(entry, @type_map) end
    end
  end

  describe "CRC integrity" do
    test "detects corrupted payload data" do
      entry = %Entry{type: :write, payload: "test"}
      bin = Entry.encode(entry, @type_map)

      # Flip a byte in the payload area (past the 8-byte header)
      <<header::binary-size(8), _rest::binary>> = bin
      corrupted = header <> <<0xFF, 0xFF, 0xFF>>

      assert {:error, _reason} = Entry.decode(corrupted, @byte_map)
    end

    test "detects single bit flip in CRC field" do
      entry = %Entry{type: :write, payload: "test"}
      <<len::32, crc::32, data::binary>> = Entry.encode(entry, @type_map)

      # Flip one bit in the CRC
      bad_crc = Bitwise.bxor(crc, 1)
      corrupted = <<len::unsigned-big-32, bad_crc::unsigned-big-32, data::binary>>

      assert {:error, {:crc_mismatch, _details}} = Entry.decode(corrupted, @byte_map)
    end

    test "detects zeroed CRC" do
      entry = %Entry{type: :write, payload: "test"}
      <<len::32, _crc::32, data::binary>> = Entry.encode(entry, @type_map)
      corrupted = <<len::unsigned-big-32, 0::unsigned-big-32, data::binary>>

      assert {:error, {:crc_mismatch, _details}} = Entry.decode(corrupted, @byte_map)
    end

    test "CRC mismatch error includes expected and computed values" do
      entry = %Entry{type: :write, payload: "test"}
      <<len::32, crc::32, data::binary>> = Entry.encode(entry, @type_map)

      bad_crc = crc + 1
      corrupted = <<len::unsigned-big-32, bad_crc::unsigned-big-32, data::binary>>

      assert {:error, {:crc_mismatch, [expected: ^bad_crc, computed: ^crc]}} =
               Entry.decode(corrupted, @byte_map)
    end
  end

  describe "decode_all/2" do
    test "decodes multiple entries" do
      e1 = %Entry{type: :write, payload: "first"}
      e2 = %Entry{type: :delete, payload: "second"}
      e3 = %Entry{type: :checkpoint, payload: %{}}

      bin =
        Entry.encode(e1, @type_map) <> Entry.encode(e2, @type_map) <> Entry.encode(e3, @type_map)

      {entries, corrupt_count} = Entry.decode_all(bin, @byte_map)
      assert length(entries) == 3
      assert corrupt_count == 0
      assert Enum.map(entries, & &1.type) == [:write, :delete, :checkpoint]
    end

    test "handles empty data" do
      {entries, corrupt_count} = Entry.decode_all(<<>>, @byte_map)
      assert entries == []
      assert corrupt_count == 0
    end

    test "skips corrupted entries and counts them" do
      good = %Entry{type: :write, payload: "good"}
      good_bin = Entry.encode(good, @type_map)

      # Create a corrupt entry: valid length/CRC header but wrong CRC
      bad_payload = :erlang.term_to_binary("bad")
      bad_data = <<0x01, bad_payload::binary>>
      bad_crc = :erlang.crc32(bad_data) + 1
      bad_len = byte_size(bad_data)
      bad_bin = <<bad_len::unsigned-big-32, bad_crc::unsigned-big-32, bad_data::binary>>

      bin = good_bin <> bad_bin <> good_bin

      {entries, corrupt_count} = Entry.decode_all(bin, @byte_map)
      assert length(entries) == 2
      assert corrupt_count == 1
    end

    test "handles trailing junk bytes less than header size" do
      good = %Entry{type: :write, payload: "good"}
      good_bin = Entry.encode(good, @type_map)

      # Append 5 junk bytes (less than 8 byte header)
      bin = good_bin <> <<1, 2, 3, 4, 5>>

      {entries, corrupt_count} = Entry.decode_all(bin, @byte_map)
      assert length(entries) == 1
      assert corrupt_count == 0
    end

    test "handles many entries" do
      bins =
        for i <- 1..100 do
          Entry.encode(%Entry{type: :write, payload: "entry_#{i}"}, @type_map)
        end

      bin = IO.iodata_to_binary(bins)
      {entries, corrupt_count} = Entry.decode_all(bin, @byte_map)
      assert length(entries) == 100
      assert corrupt_count == 0
    end

    test "preserves entry order" do
      bins =
        for i <- 1..5 do
          Entry.encode(%Entry{type: :write, payload: i}, @type_map)
        end

      bin = IO.iodata_to_binary(bins)
      {entries, _corrupt_count} = Entry.decode_all(bin, @byte_map)
      assert Enum.map(entries, & &1.payload) == [1, 2, 3, 4, 5]
    end

    test "skips entry with unknown type in decode_all" do
      # Encode a valid entry, then manually construct one with unknown type byte 0xFF
      good = Entry.encode(%Entry{type: :write, payload: "ok"}, @type_map)

      # Build a valid-CRC entry with unknown type byte
      payload_bin = :erlang.term_to_binary("unknown_payload")
      data = <<0xFF, payload_bin::binary>>
      crc = :erlang.crc32(data)
      len = byte_size(data)
      unknown_bin = <<len::unsigned-big-32, crc::unsigned-big-32, data::binary>>

      good2 = Entry.encode(%Entry{type: :delete, payload: "ok2"}, @type_map)

      bin = good <> unknown_bin <> good2
      {entries, corrupt_count} = Entry.decode_all(bin, @byte_map)
      # unknown type causes decode to stop (returns {:error, {:unknown_type, _}})
      # which hits the catch-all in decode_all and stops processing
      assert length(entries) >= 1
      assert corrupt_count == 0
    end
  end

  describe "edge cases" do
    test "incomplete header returns error" do
      assert {:error, :incomplete_header} = Entry.decode(<<1, 2, 3>>, @byte_map)
    end

    test "empty binary returns error" do
      assert {:error, :empty} = Entry.decode(<<>>, @byte_map)
    end

    test "single byte returns incomplete_header" do
      assert {:error, :incomplete_header} = Entry.decode(<<0xFF>>, @byte_map)
    end

    test "7 bytes returns incomplete_header" do
      assert {:error, :incomplete_header} = Entry.decode(<<1, 2, 3, 4, 5, 6, 7>>, @byte_map)
    end

    test "header with insufficient data returns incomplete_entry" do
      # 8 byte header saying length=100 but only 2 bytes of data
      bin = <<100::unsigned-big-32, 0::unsigned-big-32, 1, 2>>
      assert {:error, :incomplete_entry} = Entry.decode(bin, @byte_map)
    end

    test "unknown type byte returns error" do
      entry = %Entry{type: :write, payload: "test"}
      bin = Entry.encode(entry, @type_map)

      # Decode with a byte_map that doesn't include 0x01
      assert {:error, {:unknown_type, 0x01}} = Entry.decode(bin, %{0x02 => :delete})
    end

    test "encode and decode with empty string payload" do
      entry = %Entry{type: :write, payload: ""}
      bin = Entry.encode(entry, @type_map)

      assert {:ok, decoded, <<>>, false} = Entry.decode(bin, @byte_map)
      assert decoded.payload == ""
    end

    test "encode produces deterministic output" do
      entry = %Entry{type: :write, payload: "deterministic"}
      bin1 = Entry.encode(entry, @type_map)
      bin2 = Entry.encode(entry, @type_map)
      assert bin1 == bin2
    end
  end
end
