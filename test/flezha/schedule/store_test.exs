defmodule Flezha.Schedule.StoreTest do
  # async: false: a shared on-disk root; registered singletons.
  use ExUnit.Case, async: false

  alias Flezha.Identificator
  alias Flezha.Schedule.Store
  alias FlezhaTest.Helper.ScheduleTestHelper

  setup do
    ScheduleTestHelper.isolated_store_setup()
  end

  defp fields(overrides \\ %{}) do
    Map.merge(
      %{
        name: "digest",
        cron: "0 8 * * *",
        timezone: "Europe/Tallinn",
        factory_id: "ai_digest",
        namespace: "default",
        init_values: %{"mode" => "daily"}
      },
      overrides
    )
  end

  test "add, get, list, remove round-trip" do
    assert {:ok, entry} = Store.add(fields())
    assert {:ok, _, _} = DateTime.from_iso8601(entry.created_at)

    assert {:ok, ^entry} = Store.get("digest")
    assert [^entry] = Store.list()

    assert :ok = Store.remove("digest")
    assert :error = Store.get("digest")
    assert {:error, {:schedule_not_found, "digest"}} = Store.remove("digest")
  end

  test "the plugin spec round-trips verbatim and defaults to none" do
    spec =
      ~s([{"id":"reports","provider":"operator_http_push","hooks":["on_output"]}])

    assert {:ok, entry} = Store.add(fields(%{plugins: spec}))
    assert entry.plugins == spec
    assert {:ok, stored} = Store.get("digest")
    assert stored.plugins == spec

    assert :ok = Store.remove("digest")
    assert {:ok, bare} = Store.add(fields())
    assert bare.plugins == "[]"
  end

  test "the first message and its channel round-trip verbatim and default to none" do
    assert {:ok, entry} = Store.add(fields(%{text: ~s({"day":"today"}), channel: "requests"}))
    assert {:ok, %{text: ~s({"day":"today"}), channel: "requests"}} = Store.get(entry.name)

    assert {:ok, bare} = Store.add(fields(%{name: "bare"}))
    assert {:ok, %{text: nil, channel: nil}} = Store.get(bare.name)
  end

  test "a duplicate name is rejected" do
    assert {:ok, _} = Store.add(fields())
    assert {:error, {:schedule_exists, _name}} = Store.add(fields())
  end

  test "validation failures store nothing and name the invalid field" do
    assert {:error, {:cron_invalid_field, _cron, "minute", "61", 0, 59}} =
             Store.add(fields(%{cron: "61 * * * *"}))

    assert {:error, {:invalid_timezone, "Mars/Olympus"}} =
             Store.add(fields(%{timezone: "Mars/Olympus"}))

    assert {:error, {:invalid_schedule_name, "has spaces", 11}} =
             Store.add(fields(%{name: "has spaces"}))

    assert {:error, {:invalid_schedule_name, "a@b", 11}} = Store.add(fields(%{name: "a@b"}))
    # 12 bytes composes `<name>@<slot>` beyond the 32-byte operator id limit.
    assert {:error, {:invalid_schedule_name, "daily_digest", 11}} =
             Store.add(fields(%{name: "daily_digest"}))

    assert {:error, :factory_required} = Store.add(fields(%{factory_id: ""}))
    assert {:error, :init_values_not_object} = Store.add(fields(%{init_values: "nope"}))
    assert {:error, :channel_without_text} = Store.add(fields(%{channel: "requests"}))
    assert {:error, :first_message_not_text} = Store.add(fields(%{text: 42}))

    assert {:error, :plugins_not_array} =
             Store.add(fields(%{plugins: "{\"not\":\"an array\"}"}))

    assert {:error, :plugins_not_array} = Store.add(fields(%{plugins: "not json"}))

    assert Store.list() == []
  end

  test "the longest accepted name composes ids within the operator id limit" do
    name = String.duplicate("a", 11)
    assert {:ok, _} = Store.add(fields(%{name: name}))

    assert :ok = Identificator.validate(:operator, "#{name}@2026-07-27T05:00:00Z")

    assert :ok =
             Identificator.validate(:operator, "#{name}@manual-#{System.system_time(:second)}")
  end

  test "entries survive a store restart" do
    assert {:ok, _} = Store.add(fields())

    :ok = stop_supervised!(Store)
    start_supervised!(Store)

    assert {:ok, entry} = Store.get("digest")
    assert entry.factory_id == "ai_digest"
    assert entry.cron == "0 8 * * *"
  end
end
