defmodule Flezha.Service.ConsoleAnswersTest do
  @moduledoc """
  Answers a console reads to decide what it offers: the operations
  `Flezha.Service.Namespace.whoami/1` says the caller's role grants, the
  history retention `Flezha.Service.Steps.cells/2` says a relay class runs with,
  and the typed option rows `Flezha.Service.Fleet.provider/2` answers.
  """
  # async: false: writes Application env; the auth stores are node-global singletons.
  use FlezhaTest.Case,
    fixtures: [{:auth_stores, prefix: "flz_console_answers", claim: true}, :env],
    async: false

  import ExUnit.CaptureIO

  alias Flezha.AuthZ
  alias Flezha.Config
  alias Flezha.Plugin.Providers
  alias Flezha.Service.Admin
  alias Flezha.Service.Cli
  alias Flezha.Service.Fleet
  alias Flezha.Service.Namespace
  alias Flezha.Service.Steps

  describe "whoami/1 operations" do
    test "each role answers exactly the operations it is granted" do
      for {role, namespace} <- [viewer: "team-orders", ops: "team-orders", admin: "_system"] do
        context = bound(role, namespace)
        assert {:ok, %{role: ^role, operations: operations}} = Namespace.whoami(context)
        assert operations == Enum.map(Admin.catalogue(context), & &1.name)
      end
    end

    test "the lists differ where the roles do" do
      operations = fn role, namespace ->
        {:ok, %{operations: operations}} = Namespace.whoami(bound(role, namespace))
        operations
      end

      viewer = operations.(:viewer, "team-orders")
      ops = operations.(:ops, "team-orders")
      admin = operations.(:admin, "_system")

      assert "Relay.list" in viewer
      refute "Relay.start" in viewer
      assert "Relay.replay" in ops
      refute "Steps.delete" in ops
      assert "Steps.delete" in admin
      assert "Namespace.issue_token" in admin
    end

    test "flz auth whoami prints the identity and nothing more" do
      identity = %{principal: "carol", namespace: "team-orders", role: :ops, operations: ["x"]}
      output = capture_io(fn -> Cli.Authz.whoami(identity, [], []) end)

      assert output == "principal: carol\nnamespace: team-orders\nrole:      ops\n"
    end
  end

  test "an unbound caller under :open answers every operation it reaches" do
    open()

    context = %{source: :cli}
    assert {:ok, %{principal: nil, operations: operations}} = Namespace.whoami(context)
    assert operations == Enum.map(Admin.reachable(context), & &1.name)
    assert "Steps.delete" in operations
  end

  describe "cells/2 history retention" do
    setup do
      open()
    end

    test "answers the default until set_retention/3 changes it" do
      context = %{source: :cli}

      assert {:ok, %{history_retention_hours: default}} = Steps.cells("console_rc", context)
      assert default == Config.get(:default_time_travel_retention_hours)

      assert {:ok, %{hours: 7}} = Steps.set_retention("console_rc", 7, context)
      assert {:ok, %{history_retention_hours: 7}} = Steps.cells("console_rc", context)
    end
  end

  defmodule OptionProbe do
    @moduledoc false

    use Flezha.Plugin.ProviderBehaviour,
      options: [
        batch: [type: :int, min: 1, max: 10, default: 5, doc: "Batch size"],
        enabled: [type: :bool, default: true, doc: "On or off"],
        url: [type: :string, required: true, doc: "Where to go"],
        topics: [type: :names, default: ["a", "b"], doc: "Topics"],
        codec: [type: :enum, values: [:none, :gzip], default: :none, doc: "Codec"],
        owner: [type: :pid, doc: "Owner"],
        body: [type: :json, doc: "Body"],
        headers: [type: :map, default: %{}, doc: "Headers"],
        extra: [type: :list, default: [], live: true, doc: "Extra"],
        blob: [type: :any, doc: "Blob"],
        untyped: [doc: "Untyped"],
        hidden: [type: :string, internal: true, default: "x"],
        secret: [type: :string, transient: true, default: "s3cret", doc: "Secret"]
      ]

    @impl true
    def reconfigure(_old, _new), do: :ok
  end

  describe "Fleet.provider/2 options" do
    setup do
      open()
      :ok = Providers.add("console_option_probe", OptionProbe)
      on_exit(&Providers.reset_installed/0)
      :ok
    end

    test "answers every public row, typed, in declaration order" do
      assert {:ok, answer} = Fleet.provider("console_option_probe", %{source: :cli})

      assert answer.options == [
               %{
                 name: "batch",
                 type: :int,
                 required: false,
                 default: 5,
                 min: 1,
                 max: 10,
                 doc: "Batch size"
               },
               %{name: "enabled", type: :bool, required: false, default: true, doc: "On or off"},
               %{name: "url", type: :string, required: true, doc: "Where to go"},
               %{
                 name: "topics",
                 type: :names,
                 required: false,
                 default: ["a", "b"],
                 doc: "Topics"
               },
               %{
                 name: "codec",
                 type: :enum,
                 required: false,
                 default: :none,
                 values: [:none, :gzip],
                 doc: "Codec"
               },
               %{name: "owner", type: :pid, required: false, doc: "Owner"},
               %{name: "body", type: :json, required: false, doc: "Body"},
               %{name: "headers", type: :map, required: false, default: %{}, doc: "Headers"},
               %{
                 name: "extra",
                 type: :list,
                 required: false,
                 default: [],
                 live: true,
                 doc: "Extra"
               },
               %{name: "blob", type: :any, required: false, doc: "Blob"},
               %{name: "untyped", type: :any, required: false, doc: "Untyped"},
               %{name: "secret", type: :string, required: false, transient: true, doc: "Secret"}
             ]

      refute Enum.any?(answer.options, &(&1.name == "hidden"))
    end

    test "keeps the keys it answered before" do
      assert {:ok, answer} = Fleet.provider("console_option_probe", %{source: :cli})

      assert %{provider: "console_option_probe", type: :relay, hooks: []} = answer
      assert answer.help == OptionProbe.help("console_option_probe")
      assert Map.keys(answer) |> Enum.sort() == [:help, :hooks, :options, :provider, :type]
    end

    test "the door writes atoms as their names and changes nothing else" do
      assert {:ok, answer} = Fleet.provider("console_option_probe", %{source: :cli})

      assert {:ok, json} =
               Admin.call(
                 "Fleet.provider",
                 Jason.encode!(["console_option_probe"]),
                 %{source: :cli},
                 :given
               )

      decoded = Jason.decode!(json)
      assert Enum.sort(Map.keys(decoded)) == ["help", "hooks", "options", "provider", "type"]
      assert length(decoded["options"]) == length(answer.options)

      for {term, written} <- Enum.zip(answer.options, decoded["options"]) do
        assert Enum.sort(Map.keys(written)) ==
                 term |> Map.keys() |> Enum.map(&to_string/1) |> Enum.sort()

        assert written["name"] == term.name
        assert written["type"] == Atom.to_string(term.type)
      end

      assert %{"type" => "int", "min" => 1, "max" => 10} = Enum.at(decoded["options"], 0)

      assert %{"values" => ["none", "gzip"], "default" => "none"} =
               Enum.find(decoded["options"], &(&1["name"] == "codec"))
    end

    test "every provider answers, and every default it declares is JSON" do
      for {id, _module} <- Providers.all() do
        assert {:ok, %{options: options}} = Fleet.provider(id, %{source: :cli})

        for option <- options, Map.has_key?(option, :default) do
          assert json_term?(option.default),
                 "#{id}.#{option.name} declares a default JSON cannot carry: " <>
                   inspect(option.default)
        end
      end
    end
  end

  defp json_term?(value) when is_atom(value) or is_number(value) or is_binary(value), do: true
  defp json_term?(value) when is_list(value), do: Enum.all?(value, &json_term?/1)

  defp json_term?(%{} = value) when not is_struct(value),
    do: Enum.all?(value, fn {key, nested} -> json_term?(key) and json_term?(nested) end)

  defp json_term?(_value), do: false

  defp open do
    config = Application.get_env(:flezha, :authn_authz, [])
    Application.put_env(:flezha, :authn_authz, Keyword.put(config, :enabled, false))
    assert AuthZ.enforcement_posture() == :open
    :ok
  end

  defp bound(role, namespace),
    do: %{source: :cli, principal: "#{role}-caller", namespace: namespace, role: role}
end
