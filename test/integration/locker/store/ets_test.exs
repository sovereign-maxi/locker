defmodule Locker.Store.ETSTest do
  use ExUnit.Case, async: false

  alias Locker.Store.ETS

  setup do
    name = :"ets_test_#{:erlang.unique_integer([:positive])}"
    {:ok, ref} = ETS.init(name: name)

    on_exit(fn ->
      try do
        ETS.close(ref)
      rescue
        _error -> :ok
      end
    end)

    {:ok, ref: ref}
  end

  describe "init/1" do
    test "creates a named ETS table" do
      name = :"ets_init_test_#{:erlang.unique_integer([:positive])}"
      {:ok, ref} = ETS.init(name: name)
      assert :ets.info(ref, :size) == 0
      ETS.close(ref)
    end

    test "respects access option" do
      name = :"ets_access_test_#{:erlang.unique_integer([:positive])}"
      {:ok, ref} = ETS.init(name: name, access: :protected)
      assert :ets.info(ref, :protection) == :protected
      ETS.close(ref)
    end

    test "defaults to public access" do
      name = :"ets_default_access_#{:erlang.unique_integer([:positive])}"
      {:ok, ref} = ETS.init(name: name)
      assert :ets.info(ref, :protection) == :public
      ETS.close(ref)
    end
  end

  describe "put/3 and get/3" do
    test "put and get", %{ref: ref} do
      assert :ok = ETS.put(ref, "key1", "value1")
      assert {:ok, "value1"} = ETS.get(ref, "key1")
    end

    test "get returns :not_found for missing key", %{ref: ref} do
      assert :not_found = ETS.get(ref, "missing")
    end

    test "put overwrites existing value", %{ref: ref} do
      ETS.put(ref, "key1", "first")
      ETS.put(ref, "key1", "second")
      assert {:ok, "second"} = ETS.get(ref, "key1")
    end

    test "supports atom keys", %{ref: ref} do
      ETS.put(ref, :atom_key, "val")
      assert {:ok, "val"} = ETS.get(ref, :atom_key)
    end

    test "supports integer keys", %{ref: ref} do
      ETS.put(ref, 42, "val")
      assert {:ok, "val"} = ETS.get(ref, 42)
    end

    test "supports complex values", %{ref: ref} do
      complex = %{nested: [1, 2, %{deep: true}]}
      ETS.put(ref, "key", complex)
      assert {:ok, ^complex} = ETS.get(ref, "key")
    end

    test "supports nil value", %{ref: ref} do
      ETS.put(ref, "nil_key", nil)
      assert {:ok, nil} = ETS.get(ref, "nil_key")
    end

    test "get on closed table returns error" do
      name = :"ets_closed_#{:erlang.unique_integer([:positive])}"
      {:ok, ref} = ETS.init(name: name)
      ETS.close(ref)
      assert {:error, :table_not_found} = ETS.get(ref, "key")
    end
  end

  describe "member?/2" do
    test "member?", %{ref: ref} do
      refute ETS.member?(ref, "key1")
      ETS.put(ref, "key1", true)
      assert ETS.member?(ref, "key1")
    end

    test "member? returns false for closed table" do
      name = :"ets_member_closed_#{:erlang.unique_integer([:positive])}"
      {:ok, ref} = ETS.init(name: name)
      ETS.close(ref)
      refute ETS.member?(ref, "key")
    end
  end

  describe "delete/2" do
    test "delete", %{ref: ref} do
      ETS.put(ref, "key1", "val")
      assert :ok = ETS.delete(ref, "key1")
      assert :not_found = ETS.get(ref, "key1")
    end

    test "delete non-existent key succeeds", %{ref: ref} do
      assert :ok = ETS.delete(ref, "nonexistent")
    end

    test "delete then member? returns false", %{ref: ref} do
      ETS.put(ref, "key", "val")
      ETS.delete(ref, "key")
      refute ETS.member?(ref, "key")
    end
  end

  describe "size/1" do
    test "size", %{ref: ref} do
      assert ETS.size(ref) == 0
      ETS.put(ref, "a", 1)
      ETS.put(ref, "b", 2)
      assert ETS.size(ref) == 2
    end

    test "size after delete", %{ref: ref} do
      ETS.put(ref, "a", 1)
      ETS.put(ref, "b", 2)
      ETS.delete(ref, "a")
      assert ETS.size(ref) == 1
    end

    test "size with overwrites does not increase", %{ref: ref} do
      ETS.put(ref, "a", 1)
      ETS.put(ref, "a", 2)
      assert ETS.size(ref) == 1
    end
  end

  describe "put_batch/2" do
    test "put_batch", %{ref: ref} do
      assert :ok = ETS.put_batch(ref, [{"a", 1}, {"b", 2}, {"c", 3}])
      assert ETS.size(ref) == 3
      assert {:ok, 2} = ETS.get(ref, "b")
    end

    test "put_batch with empty list", %{ref: ref} do
      assert :ok = ETS.put_batch(ref, [])
      assert ETS.size(ref) == 0
    end

    test "put_batch overwrites existing keys", %{ref: ref} do
      ETS.put(ref, "a", 1)
      ETS.put_batch(ref, [{"a", 99}, {"b", 2}])
      assert {:ok, 99} = ETS.get(ref, "a")
    end
  end

  describe "to_list/1" do
    test "to_list", %{ref: ref} do
      ETS.put(ref, "x", 10)
      ETS.put(ref, "y", 20)

      list = ETS.to_list(ref)
      assert length(list) == 2
      assert {"x", 10} in list
      assert {"y", 20} in list
    end

    test "to_list on empty table", %{ref: ref} do
      assert ETS.to_list(ref) == []
    end
  end

  describe "close/1" do
    test "close is idempotent" do
      name = :"ets_close_idem_#{:erlang.unique_integer([:positive])}"
      {:ok, ref} = ETS.init(name: name)
      assert :ok = ETS.close(ref)
      assert :ok = ETS.close(ref)
    end
  end
end
