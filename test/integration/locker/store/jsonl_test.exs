defmodule Locker.Store.JSONLTest do
  use ExUnit.Case, async: true

  alias Locker.Store.JSONL

  setup do
    dir = Path.join(System.tmp_dir!(), "locker_jsonl_#{:erlang.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    path = Path.join(dir, "test.jsonl")

    on_exit(fn -> File.rm_rf!(dir) end)

    {:ok, ref} = JSONL.init(path: path)
    {:ok, ref: ref, path: path}
  end

  test "put and get", %{ref: ref} do
    assert :ok = JSONL.put(ref, "key1", "value1")
    assert {:ok, "value1"} = JSONL.get(ref, "key1")
  end

  test "get returns :not_found for missing key", %{ref: ref} do
    assert :not_found = JSONL.get(ref, "missing")
  end

  test "last write wins", %{ref: ref} do
    JSONL.put(ref, "key1", "first")
    JSONL.put(ref, "key1", "second")
    JSONL.put(ref, "key1", "third")

    assert {:ok, "third"} = JSONL.get(ref, "key1")
  end

  test "member?", %{ref: ref} do
    refute JSONL.member?(ref, "key1")
    JSONL.put(ref, "key1", true)
    assert JSONL.member?(ref, "key1")
  end

  test "delete rewrites file without key", %{ref: ref} do
    JSONL.put(ref, "a", 1)
    JSONL.put(ref, "b", 2)

    assert :ok = JSONL.delete(ref, "a")
    assert :not_found = JSONL.get(ref, "a")
    assert {:ok, 2} = JSONL.get(ref, "b")
  end

  test "size", %{ref: ref} do
    assert JSONL.size(ref) == 0
    JSONL.put(ref, "a", 1)
    JSONL.put(ref, "b", 2)
    assert JSONL.size(ref) == 2
  end

  test "put_batch", %{ref: ref} do
    JSONL.put_batch(ref, [{"a", 1}, {"b", 2}, {"c", 3}])

    assert JSONL.size(ref) == 3
    assert {:ok, 2} = JSONL.get(ref, "b")
  end

  test "to_list", %{ref: ref} do
    JSONL.put(ref, "x", 10)
    JSONL.put(ref, "y", 20)

    list = JSONL.to_list(ref)
    assert length(list) == 2
    assert {"x", 10} in list
    assert {"y", 20} in list
  end

  test "file is human-readable", %{ref: ref, path: path} do
    JSONL.put(ref, "hello", "world")
    content = File.read!(path)
    assert content =~ "hello"
    assert content =~ "world"
    assert String.ends_with?(content, "\n")
  end
end
