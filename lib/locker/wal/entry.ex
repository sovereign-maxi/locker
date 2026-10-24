defmodule Locker.WAL.Entry do
  @moduledoc """
  Binary encoding and decoding for WAL entries.

  ## Format

      | Field   | Size     | Description                          |
      |---------|----------|--------------------------------------|
      | length  | 4 bytes  | Big-endian uint32, length of payload |
      | crc32   | 4 bytes  | Big-endian uint32, CRC32 checksum    |
      | type    | 1 byte   | Entry type identifier                |
      | payload | variable | Erlang term_to_binary encoded data   |

  CRC32 is computed over `type <> payload` (excludes length and CRC fields).
  CRC32 catches accidental corruption (bitrot, torn writes); it does
  not provide tamper-detection. Callers requiring authenticity should
  encrypt or HMAC payloads before encoding.

  ## Type Registry

  Entry types are registered at WAL startup via a type map:

      %{keyset_created: 0x01, proof_spent: 0x02, checkpoint: 0x06, ...}

  This allows each application to define its own entry types without
  modifying Locker. The only reserved type is `:checkpoint` (0x06).
  """

  defstruct [:type, :payload, :crc]

  @type t :: %__MODULE__{
          type: atom(),
          payload: term(),
          crc: non_neg_integer() | nil
        }

  # Maximum encoded entry size (16 MB). Prevents memory exhaustion
  # from corrupted length fields or crafted payloads.
  @max_entry_bytes 16 * 1024 * 1024

  @doc """
  Encodes an entry into its binary representation.

  The `type_map` maps atom types to byte identifiers.
  Raises if encoded payload exceeds 16 MB.
  """
  @spec encode(t(), %{atom() => non_neg_integer()}) :: binary()
  def encode(%__MODULE__{type: type, payload: payload}, type_map) do
    type_byte = Map.fetch!(type_map, type)
    payload_bin = :erlang.term_to_binary(payload)
    data = <<type_byte::8, payload_bin::binary>>

    if byte_size(data) > @max_entry_bytes do
      raise ArgumentError, "WAL entry exceeds maximum size of #{@max_entry_bytes} bytes"
    end

    crc = :erlang.crc32(data)
    length = byte_size(data)

    <<length::unsigned-big-32, crc::unsigned-big-32, data::binary>>
  end

  @doc """
  Decodes a single entry from binary data.

  Returns `{:ok, entry, rest, corrupted?}` on success or `{:error, reason}`.
  """
  @spec decode(binary(), %{non_neg_integer() => atom()}) ::
          {:ok, t(), binary(), boolean()} | {:error, term()}
  def decode(
        <<length::unsigned-big-32, _crc::unsigned-big-32, _rest::binary>>,
        _byte_map
      )
      when length > 16 * 1024 * 1024 do
    {:error, {:entry_too_large, length}}
  end

  def decode(
        <<length::unsigned-big-32, crc::unsigned-big-32, rest::binary>>,
        byte_map
      )
      when byte_size(rest) >= length do
    <<data::binary-size(^length), remaining::binary>> = rest
    computed_crc = :erlang.crc32(data)

    if computed_crc == crc do
      <<type_byte::8, payload_bin::binary>> = data

      case Map.fetch(byte_map, type_byte) do
        {:ok, type} ->
          {payload, corrupted?} =
            try do
              {:erlang.binary_to_term(payload_bin, [:safe]), false}
            rescue
              _error -> {{:__corrupted__, payload_bin}, true}
            end

          entry = %__MODULE__{type: type, payload: payload, crc: crc}
          {:ok, entry, remaining, corrupted?}

        :error ->
          {:error, {:unknown_type, type_byte}}
      end
    else
      {:error, {:crc_mismatch, expected: crc, computed: computed_crc}}
    end
  end

  def decode(<<_length::unsigned-big-32, _crc::unsigned-big-32, _rest::binary>>, _byte_map) do
    {:error, :incomplete_entry}
  end

  def decode(partial, _byte_map) when is_binary(partial) and byte_size(partial) < 8 do
    if byte_size(partial) == 0, do: {:error, :empty}, else: {:error, :incomplete_header}
  end

  @doc """
  Decodes all entries from binary data, skipping corrupted entries.

  Returns `{entries, corrupt_count}`.
  """
  @spec decode_all(binary(), %{non_neg_integer() => atom()}) :: {[t()], non_neg_integer()}
  def decode_all(data, byte_map) do
    decode_all(data, byte_map, [], 0)
  end

  defp decode_all(<<>>, _byte_map, acc, corrupt_count) do
    {Enum.reverse(acc), corrupt_count}
  end

  defp decode_all(data, _byte_map, acc, corrupt_count) when byte_size(data) < 8 do
    {Enum.reverse(acc), corrupt_count}
  end

  defp decode_all(data, byte_map, acc, corrupt_count) do
    case decode(data, byte_map) do
      {:ok, entry, rest, false} ->
        decode_all(rest, byte_map, [entry | acc], corrupt_count)

      {:ok, entry, rest, true} ->
        decode_all(rest, byte_map, [entry | acc], corrupt_count + 1)

      {:error, {:crc_mismatch, _details}} ->
        skip_current_entry(data, byte_map, acc, corrupt_count)

      # Unknown type byte: the writer used a type this reader doesn't
      # know (older release, dropped type). We can still trust the
      # length field because the type byte lives INSIDE the CRC-
      # covered `data` window — if length were wrong the CRC would
      # have mismatched. Skip past this entry and keep going so the
      # single unknown doesn't truncate every entry that follows it.
      {:error, {:unknown_type, _type_byte}} ->
        skip_current_entry(data, byte_map, acc, corrupt_count)

      # These three tell us we cannot safely advance the cursor.
      # Terminate the loop AND count the corruption so the caller
      # can fire the halt-on-corrupt path — the previous silent
      # return let a single bad-length field discard every following
      # entry without triggering the operator's halt guard.
      {:error, {:entry_too_large, _length}} ->
        {Enum.reverse(acc), corrupt_count + 1}

      {:error, :incomplete_entry} ->
        {Enum.reverse(acc), corrupt_count + 1}

      {:error, :incomplete_header} ->
        {Enum.reverse(acc), corrupt_count + 1}

      {:error, _reason} ->
        {Enum.reverse(acc), corrupt_count}
    end
  end

  defp skip_current_entry(data, byte_map, acc, corrupt_count) do
    <<length::unsigned-big-32, _crc::unsigned-big-32, rest::binary>> = data

    if byte_size(rest) >= length do
      <<_skipped::binary-size(^length), remaining::binary>> = rest
      decode_all(remaining, byte_map, acc, corrupt_count + 1)
    else
      {Enum.reverse(acc), corrupt_count + 1}
    end
  end
end
