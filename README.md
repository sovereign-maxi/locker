# Locker

Composable storage primitives for Elixir applications.

## Installation

Add `locker` to your dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:locker, path: "../locker"}
  ]
end
```

## Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                              locker                             │
│                                                                 │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐   │
│  │     WAL      │  │   Counter    │  │       Store          │   │
│  │ (write-ahead)│  │ (ETS+DETS)   │  │    (behaviour)       │   │
│  └──────────────┘  └──────────────┘  └──────────────────────┘   │
│                                                                 │
│  WAL.Entry                           Store backends:            │
│  (binary codec)                      ┌──────┐ ┌──────┐          │
│                                      │  ETS │ │ DETS │          │
│                                      └──────┘ └──────┘          │
│                                      ┌──────┐ ┌──────┐          │
│                                      │CubDB │ │JSONL │          │
│                                      └──────┘ └──────┘          │
└─────────────────────────────────────────────────────────────────┘
```

Three independent components. Use what you need.

## Modules

| Module | Purpose |
|--------|---------|
| `Locker.WAL` | Write-Ahead Log GenServer with CRC32 integrity and segment rollover |
| `Locker.WAL.Entry` | Binary entry codec with configurable type registry |
| `Locker.Counter` | Atomic counters backed by ETS (fast reads) and DETS (durability) |
| `Locker.Store` | Behaviour for pluggable key-value storage backends |
| `Locker.Store.ETS` | In-memory backend, fastest reads, no persistence |
| `Locker.Store.DETS` | File-backed backend, persistent, 2GB limit |
| `Locker.Store.CubDB` | Pure Elixir B-tree, ACID transactions, no size limit |
| `Locker.Store.JSONL` | Append-only JSON lines, human-readable, good for audit trails |

## Usage

### Write-Ahead Log

All state mutations written before applied. CRC32 integrity, segment rollover,
deterministic replay. If the process crashes, the WAL is the source of truth.

```elixir
# Define your entry types
types = %{write: 0x01, delete: 0x02, checkpoint: 0x06}

{:ok, _pid} = Locker.WAL.start_link(
  wal_dir: "/data/wal",
  types: types
)

# Append entries (fsynced before returning)
alias Locker.WAL
alias Locker.WAL.Entry

WAL.append(%Entry{type: :write, payload: %{key: "k1", value: "v1"}})
WAL.append(%Entry{type: :delete, payload: %{key: "k1"}})

# Read all entries
{:ok, entries} = WAL.read_all()

# Checkpoint (marks committed)
WAL.checkpoint()

# Replay from disk without GenServer (for recovery)
byte_map = %{0x01 => :write, 0x02 => :delete, 0x06 => :checkpoint}
{:ok, entries, corrupt_count} = WAL.replay("/data/wal", byte_map)

# Maintenance
WAL.rotate()           # Force segment rotation
WAL.prune("/data/wal") # Keep 5 most recent segments
WAL.truncate("/data/wal") # Remove all segments
```

### Entry Codec

```elixir
alias Locker.WAL.Entry

type_map = %{write: 0x01, checkpoint: 0x06}
byte_map = %{0x01 => :write, 0x06 => :checkpoint}

# Encode
entry = %Entry{type: :write, payload: %{key: "foo"}}
bin = Entry.encode(entry, type_map)

# Decode
{:ok, decoded, remaining, corrupted?} = Entry.decode(bin, byte_map)

# Decode all (skips corrupt entries)
{entries, corrupt_count} = Entry.decode_all(bin, byte_map)
```

### Counter

Atomic counters with ETS for fast reads and DETS for durability.
Lock-free increments, periodic sync, survives restarts.

```elixir
alias Locker.Counter

{:ok, _pid} = Counter.start_link(
  name: MyApp.LiabilityCounter,
  keys: [:total_minted, :total_burned],
  data_dir: "/data"
)

# Increment (returns new value)
Counter.increment(MyApp.LiabilityCounter, :total_minted, 1000)

# Read
Counter.read(MyApp.LiabilityCounter, :total_minted)     # 1000
Counter.read_all(MyApp.LiabilityCounter)                 # %{total_minted: 1000, total_burned: 0}

# Recovery
Counter.reset(MyApp.LiabilityCounter)                    # Zero all counters
Counter.restore(MyApp.LiabilityCounter, :total_minted, 5000) # Set specific value
```

### Store Backends

Pluggable key-value storage. All backends implement `Locker.Store`:

```elixir
# ETS — in-memory, fastest
{:ok, ref} = Locker.Store.ETS.init(name: :my_cache)

# DETS — file-backed, persistent, 2GB limit
{:ok, ref} = Locker.Store.DETS.init(name: :my_store, path: "/data/store.dets")

# CubDB — B-tree, ACID, no size limit
{:ok, ref} = Locker.Store.CubDB.init(path: "/data/cubdb")

# JSONL — append-only, human-readable
{:ok, ref} = Locker.Store.JSONL.init(path: "/data/audit.jsonl")
```

All backends share the same API:

```elixir
store = Locker.Store.ETS  # or DETS, CubDB, JSONL

store.put(ref, "key", "value")
{:ok, "value"} = store.get(ref, "key")
true = store.member?(ref, "key")
store.delete(ref, "key")
store.put_batch(ref, [{"a", 1}, {"b", 2}])
2 = store.size(ref)
[{"a", 1}, {"b", 2}] = store.to_list(ref)
store.close(ref)
```

## Architecture

- **WAL is the foundation** — fsync before acknowledge, CRC32 integrity, deterministic replay
- **Counter is a pattern** — ETS + DETS atomic counters, extracted from repeated implementations
- **Store is a behaviour** — swap backends without modifying upstream code
- **No opinions** — each component is independent, use what you need
- **Config via opts** — no `Application.get_env`, all configuration passed as arguments

## Development

### Pre-commit Hook

```bash
git config core.hooksPath hooks
```

### Testing

```bash
# Run all tests
mix test

# Unit tests (pure functions)
mix test test/unit/

# Integration tests (stateful GenServers)
mix test test/integration/

# Check code style
mix credo --strict
```

## Dependencies

```elixir
defp deps do
  [
    {:cubdb, "~> 2.0"},
    {:jason, "~> 1.4"}
  ]
end
```

## License

MIT
