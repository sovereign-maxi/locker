defmodule Locker.Store do
  @moduledoc """
  Behaviour for pluggable key-value storage backends.

  Implementations provide persistence with varying tradeoffs:

  - `Locker.Store.ETS` — in-memory, fastest, no persistence
  - `Locker.Store.DETS` — file-backed, 2GB limit, good for small datasets
  - `Locker.Store.CubDB` — pure Elixir B-tree, ACID, no size limit
  - `Locker.Store.JSONL` — append-only log, human-readable, good for audit trails
  """

  @type ref :: term()
  @type key :: term()
  @type value :: term()

  @doc "Initializes the backend. Returns `{:ok, ref}` or `{:error, reason}`."
  @callback init(keyword()) :: {:ok, ref()} | {:error, term()}

  @doc "Stores a key-value pair."
  @callback put(ref(), key(), value()) :: :ok | {:error, term()}

  @doc "Inserts multiple `{key, value}` entries atomically."
  @callback put_batch(ref(), [{key(), value()}]) :: :ok | {:error, term()}

  @doc "Retrieves a value by key."
  @callback get(ref(), key()) :: {:ok, value()} | :not_found | {:error, term()}

  @doc "Checks whether a key exists."
  @callback member?(ref(), key()) :: boolean()

  @doc "Deletes a key."
  @callback delete(ref(), key()) :: :ok | {:error, term()}

  @doc "Returns the number of stored entries."
  @callback size(ref()) :: non_neg_integer()

  @doc "Returns all entries as `{key, value}` tuples."
  @callback to_list(ref()) :: [{key(), value()}]

  @doc "Gracefully shuts down, flushing pending writes."
  @callback close(ref()) :: :ok | {:error, term()}
end
