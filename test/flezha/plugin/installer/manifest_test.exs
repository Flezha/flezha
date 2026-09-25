defmodule Flezha.Plugin.Installer.ManifestTest do
  @moduledoc """
  Spec-driven tests for `Flezha.Plugin.Installer.Manifest`.
  """

  use ExUnit.Case, async: true

  alias Flezha.Plugin.Installer.Manifest

  test "valid manifest parses to a term" do
    src = """
    %{
      id: "kafka_ingest",
      version: "1.2.0",
      description: "Kafka ingest",
      author: "ops",
      modules: [MyOrg.KafkaIngest],
      provides: [
        %{
          id: "ki",
          provider: MyOrg.KafkaIngest,
          hooks: [{:on_input, [position: 0]}],
          default_opts: %{},
          scope: :operator
        }
      ],
      dependencies: []
    }
    """

    assert {:ok, term} = Manifest.parse(src)
    assert term.id == "kafka_ingest"
    assert term.modules == [MyOrg.KafkaIngest]
    assert :ok = Manifest.validate(term)
  end

  test "manifest containing a call expression is rejected" do
    src = """
    %{id: System.get_env("FOO")}
    """

    assert {:error, :non_literal_ast} = Manifest.parse(src)
  end

  test "manifest with nested call in provides rejected" do
    src = """
    %{
      id: "x",
      provides: [%{default_opts: Code.eval_string("1")}]
    }
    """

    assert {:error, :non_literal_ast} = Manifest.parse(src)
  end

  test "valid manifest accepts module aliases" do
    src = "%{id: \"x\", modules: [Foo.Bar, Baz]}"
    assert {:ok, term} = Manifest.parse(src)
    assert term.modules == [Foo.Bar, Baz]
  end

  test "syntax error surfaces as :syntax" do
    assert {:error, {:syntax, _}} = Manifest.parse("%{id: }")
  end

  test "missing version reported" do
    term = %{
      id: "x",
      modules: [X],
      provides: [valid_provision(X)],
      dependencies: []
    }

    assert {:error, {:manifest_invalid, {:missing_field, :version}}} =
             Manifest.validate(term)
  end

  test "invalid id rejected" do
    term = base_term() |> Map.put(:id, "My-Plugin")

    assert {:error, {:manifest_invalid, {:invalid_id, "My-Plugin"}}} =
             Manifest.validate(term)
  end

  test "id too long rejected" do
    long = String.duplicate("a", 65)
    term = base_term() |> Map.put(:id, long)
    assert {:error, {:manifest_invalid, {:invalid_id, ^long}}} = Manifest.validate(term)
  end

  test "non-semver version rejected" do
    term = base_term() |> Map.put(:version, "beta-2")

    assert {:error, {:manifest_invalid, {:invalid_version, "beta-2"}}} =
             Manifest.validate(term)
  end

  test "provider not in modules list rejected" do
    term =
      base_term()
      |> Map.put(:provides, [
        %{
          id: "p",
          provider: Other.NotInList,
          hooks: [{:on_input, []}],
          default_opts: %{},
          scope: :operator
        }
      ])

    assert {:error, {:manifest_invalid, {:provider_not_declared, Other.NotInList}}} =
             Manifest.validate(term)
  end

  test "unsupported scope rejected" do
    term =
      base_term()
      |> Map.put(:provides, [
        base_term().provides |> hd() |> Map.put(:scope, :host_global)
      ])

    assert {:error, {:manifest_invalid, {:unsupported_scope, :host_global}}} =
             Manifest.validate(term)
  end

  # Fails when: a provision still naming its hooks `subscriptions` installs and
  # enables a plugin with no hooks.
  test "a provision without a hooks key is missing a required field" do
    provision =
      base_term().provides
      |> hd()
      |> Map.delete(:hooks)
      |> Map.put(:subscriptions, [{:on_input, []}])

    assert {:error, {:manifest_invalid, {:missing_field, :hooks}}} =
             base_term() |> Map.put(:provides, [provision]) |> Manifest.validate()
  end

  test "duplicate provision ids rejected" do
    p = base_term().provides |> hd()

    term = base_term() |> Map.put(:provides, [p, p])

    assert {:error, {:manifest_invalid, {:duplicate_provision_id, _}}} =
             Manifest.validate(term)
  end

  test "invalid dependency constraint rejected" do
    term = base_term() |> Map.put(:dependencies, [{"common", "gibberish"}])

    assert {:error, {:manifest_invalid, {:invalid_constraint, "common", "gibberish"}}} =
             Manifest.validate(term)
  end

  defp base_term do
    %{
      id: "x",
      version: "1.0.0",
      modules: [X],
      provides: [valid_provision(X)],
      dependencies: []
    }
  end

  defp valid_provision(mod) do
    %{
      id: "p",
      provider: mod,
      hooks: [{:on_input, []}],
      default_opts: %{},
      scope: :operator
    }
  end
end
