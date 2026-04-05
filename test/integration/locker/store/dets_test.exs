defmodule Locker.Store.DETSTest do
  use ExUnit.Case, async: false

  alias Locker.Store.DETS

  setup do
    dir = Path.join(System.tmp_dir!(), "locker_dets_#{:erlang.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    path = Path.join(dir, "test.dets")
    name = :"dets_test_#{:erlang.unique_integer([:positive])}"

    {:ok, ref} = DETS.init(name: name, path: path)

    on_exit(fn ->
      try do
        DETS.close(ref)
      rescue
        _error -> :ok
      end

      File.rm_rf!(dir)
    end)

    {:ok, ref: ref, dir: dir, path: path, name: name}
  end

  test "put and get", %{ref: ref} do
    assert :ok = DETS.put(ref, "key1", "value1")
    assert {:ok, "value1"} = DETS.get(ref, "key1")
  end

  test "get returns :not_found for missing key", %{ref: ref} do
    assert :not_found = DETS.get(ref, "missing")
  end

  test "member?", %{ref: ref} do
    refute DETS.member?(ref, "key1")
    DETS.put(ref, "key1", true)
    assert DETS.member?(ref, "key1")
  end

  test "delete", %{ref: ref} do
    DETS.put(ref, "key1", "val")
    assert :ok = DETS.delete(ref, "key1")
    assert :not_found = DETS.get(ref, "key1")
  end

  test "size", %{ref: ref} do
    assert DETS.size(ref) == 0
    DETS.put(ref, "a", 1)
    DETS.put(ref, "b", 2)
    assert DETS.size(ref) == 2
  end

  test "put_batch", %{ref: ref} do
    assert :ok = DETS.put_batch(ref, [{"a", 1}, {"b", 2}, {"c", 3}])
    assert DETS.size(ref) == 3
    assert {:ok, 2} = DETS.get(ref, "b")
  end

  test "to_list", %{ref: ref} do
    DETS.put(ref, "x", 10)
    DETS.put(ref, "y", 20)

    list = DETS.to_list(ref)
    assert length(list) == 2
    assert {"x", 10} in list
    assert {"y", 20} in list
  end

  test "overwrites existing key", %{ref: ref} do
    DETS.put(ref, "key", "first")
    DETS.put(ref, "key", "second")
    assert {:ok, "second"} = DETS.get(ref, "key")
    assert DETS.size(ref) == 1
  end

  test "handles binary keys", %{ref: ref} do
    key = :crypto.strong_rand_bytes(32)
    DETS.put(ref, key, "binary_key_value")
    assert {:ok, "binary_key_value"} = DETS.get(ref, key)
  end

  test "handles complex value types", %{ref: ref} do
    DETS.put(ref, "map", %{a: 1, b: [2, 3]})
    assert {:ok, %{a: 1, b: [2, 3]}} = DETS.get(ref, "map")

    DETS.put(ref, "tuple", {:ok, "done"})
    assert {:ok, {:ok, "done"}} = DETS.get(ref, "tuple")
  end

  test "persists to disk", %{ref: ref, dir: dir} do
    DETS.put(ref, "persist_key", "persist_val")
    DETS.close(ref)

    path2 = Path.join(dir, "test.dets")
    name2 = :"dets_reopen_#{:erlang.unique_integer([:positive])}"
    {:ok, ref2} = DETS.init(name: name2, path: path2)

    assert {:ok, "persist_val"} = DETS.get(ref2, "persist_key")
    DETS.close(ref2)
  end

  test "delete non-existent key is ok", %{ref: ref} do
    assert :ok = DETS.delete(ref, "nope")
  end

  test "close is idempotent", %{ref: ref} do
    assert :ok = DETS.close(ref)
  end

  test "batch with empty list", %{ref: ref} do
    assert :ok = DETS.put_batch(ref, [])
    assert DETS.size(ref) == 0
  end

  test "large number of entries", %{ref: ref} do
    entries = for i <- 1..500, do: {"key_#{i}", i}
    DETS.put_batch(ref, entries)
    assert DETS.size(ref) == 500
    assert {:ok, 250} = DETS.get(ref, "key_250")
  end
end
