defmodule Locker do
  @moduledoc """
  Composable storage primitives for Elixir applications.

  Locker provides three independent components that can be used
  separately or together:

  - `Locker.WAL` — Write-Ahead Log with CRC32 integrity, segment
    rollover, and deterministic replay. The foundation for crash-safe
    persistence.

  - `Locker.Counter` — Atomic counters backed by ETS (fast reads)
    and DETS (durability). Lock-free increments with periodic sync.

  - `Locker.Store` — Behaviour for pluggable key-value backends
    with implementations for ETS, DETS, CubDB, and JSONL.

  Each component is independent. Use what you need.
  """
end
