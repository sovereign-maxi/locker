defmodule Locker.Store.CubDBTest do
  use ExUnit.Case, async: false

  alias Locker.Store.CubDB, as: CubDBStore

  setup do
    dir = Path.join(System.tmp_dir!(), "locker_cubdb_#{:erlang.unique_integer([:positive])}")
    File.mkdir_p!(dir)

    {:ok, ref} = CubDBStore.init(path: dir)

    on_exit(fn ->
      if Process.alive?(ref), do: CubDBStore.close(ref)
      File.rm_rf!(dir)
    end)

    {:ok, ref: ref, dir: dir}
  end

  test "put and get", %{ref: ref} do
    assert :ok = CubDBStore.put(ref, "key1", "value1")
    assert {:ok, "value1"} = CubDBStore.get(ref, "key1")
  end

  test "get returns :not_found for missing key", %{ref: ref} do
    assert :not_found = CubDBStore.get(ref, "missing")
  end

  test "member?", %{ref: ref} do
    refute CubDBStore.member?(ref, "key1")
    CubDBStore.put(ref, "key1", true)
    assert CubDBStore.member?(ref, "key1")
  end

  test "delete", %{ref: ref} do
    CubDBStore.put(ref, "key1", "val")
    assert :ok = CubDBStore.delete(ref, "key1")
    assert :not_found = CubDBStore.get(ref, "key1")
  end

  test "size", %{ref: ref} do
    assert CubDBStore.size(ref) == 0
    CubDBStore.put(ref, "a", 1)
    CubDBStore.put(ref, "b", 2)
    assert CubDBStore.size(ref) == 2
  end

  test "put_batch", %{ref: ref} do
    assert :ok = CubDBStore.put_batch(ref, [{"a", 1}, {"b", 2}, {"c", 3}])
    assert CubDBStore.size(ref) == 3
    assert {:ok, 2} = CubDBStore.get(ref, "b")
  end

  test "to_list", %{ref: ref} do
    CubDBStore.put(ref, "x", 10)
    CubDBStore.put(ref, "y", 20)

    list = CubDBStore.to_list(ref)
    assert length(list) == 2
    assert {"x", 10} in list
    assert {"y", 20} in list
  end

  test "overwrites existing key", %{ref: ref} do
    CubDBStore.put(ref, "key", "first")
    CubDBStore.put(ref, "key", "second")
    assert {:ok, "second"} = CubDBStore.get(ref, "key")
    assert CubDBStore.size(ref) == 1
  end

  test "handles binary keys", %{ref: ref} do
    key = :crypto.strong_rand_bytes(32)
    CubDBStore.put(ref, key, "binary_key_value")
    assert {:ok, "binary_key_value"} = CubDBStore.get(ref, key)
  end

  test "handles complex value types", %{ref: ref} do
    CubDBStore.put(ref, "map", %{a: 1, b: [2, 3]})
    assert {:ok, %{a: 1, b: [2, 3]}} = CubDBStore.get(ref, "map")

    CubDBStore.put(ref, "tuple", {:ok, "done"})
    assert {:ok, {:ok, "done"}} = CubDBStore.get(ref, "tuple")
  end

  test "persists to disk" do
    dir =
      Path.join(System.tmp_dir!(), "locker_cubdb_persist_#{:erlang.unique_integer([:positive])}")

    File.mkdir_p!(dir)

    {:ok, ref} = CubDBStore.init(path: dir)
    CubDBStore.put(ref, "persist_key", "persist_val")
    CubDBStore.close(ref)

    {:ok, ref2} = CubDBStore.init(path: dir)
    assert {:ok, "persist_val"} = CubDBStore.get(ref2, "persist_key")
    CubDBStore.close(ref2)

    File.rm_rf!(dir)
  end

  test "delete non-existent key is ok", %{ref: ref} do
    assert :ok = CubDBStore.delete(ref, "nope")
  end

  test "batch with empty list", %{ref: ref} do
    assert :ok = CubDBStore.put_batch(ref, [])
    assert CubDBStore.size(ref) == 0
  end

  test "large number of entries", %{ref: ref} do
    entries = for i <- 1..500, do: {"key_#{i}", i}
    CubDBStore.put_batch(ref, entries)
    assert CubDBStore.size(ref) == 500
    assert {:ok, 250} = CubDBStore.get(ref, "key_250")
  end

  test "handles atom keys", %{ref: ref} do
    CubDBStore.put(ref, :atom_key, "atom_value")
    assert {:ok, "atom_value"} = CubDBStore.get(ref, :atom_key)
  end

  test "handles integer keys", %{ref: ref} do
    CubDBStore.put(ref, 42, "forty_two")
    assert {:ok, "forty_two"} = CubDBStore.get(ref, 42)
  end

  test "batch overwrites existing keys", %{ref: ref} do
    CubDBStore.put(ref, "a", "old")
    CubDBStore.put_batch(ref, [{"a", "new"}, {"b", "fresh"}])
    assert {:ok, "new"} = CubDBStore.get(ref, "a")
    assert {:ok, "fresh"} = CubDBStore.get(ref, "b")
  end
end
