defmodule Locker do
  @moduledoc """
  Composable storage primitives for Elixir applications.

  Locker provides three independent components that can be used
  separately or together:

  * `Locker.WAL`: write-ahead log with CRC32 integrity, segment
    rollover, and deterministic replay. The foundation for crash-safe
    persistence.

  * `Locker.Counter`: atomic counters backed by ETS (fast reads) and
    DETS (durability). Lock-free increments with periodic sync.

  * `Locker.Store`: behaviour for pluggable key-value backends with
    implementations for ETS, DETS, CubDB, and JSONL.

  Each component is independent. Use what you need.
  """
end
