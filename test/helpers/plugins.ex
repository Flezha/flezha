defmodule Flezha.TestHelpers.Plugins do
  @moduledoc """
  Shared helpers for installable-plugin tests:

    * `build_all_fixtures!/1` builds every canonical `.fpz` into a given
      directory via `test/fixtures/plugins/build_fixtures.exs`.
    * `build_fixture!/5` builds one minimal `.fpz` in place, for a test that
      needs a package rather than the canonical set.
    * `generate_keypair/0` returns a fresh raw Ed25519 keypair.
    * `loaded?/1`, `registered?/1` — assertions against the live code
      server and `Flezha.Plugin.Installer.Loader`.
  """

  alias Flezha.Plugin.Installer.Loader

  @fixtures_script "test/fixtures/plugins/build_fixtures.exs"

  @doc """
  Builds every canonical fixture under `out_dir`. Idempotent — overwrites
  any existing fixtures. `Code.require_file/1` loads the builder script
  once per VM so repeated calls from multiple tests don't spam
  "redefining module BuildFixtures" warnings.
  """
  @spec build_all_fixtures!(Path.t()) :: :ok
  def build_all_fixtures!(out_dir) do
    _ = Code.require_file(@fixtures_script)
    apply(:"Elixir.BuildFixtures", :build_all!, [out_dir])
    :ok
  end

  @doc """
  Build a minimal installable `.fpz` under `tmp` and return its path: one
  source file defining every module in `mods`, a manifest naming the first of
  them as its single `:on_input` provider, and matching `CHECKSUMS.sha256`.

  `opts[:deps]` are the manifest's `dependencies`, for the refusal a missing
  one produces.
  """
  @spec build_fixture!(Path.t(), binary(), binary(), [module()], keyword()) :: Path.t()
  def build_fixture!(tmp, id, version, mods, opts \\ []) do
    deps = Keyword.get(opts, :deps, [])
    fpz = Path.join(tmp, "#{id}-#{version}.fpz")

    [first_mod | _] = mods
    src_filename = Macro.underscore(first_mod) <> ".ex"

    src_body = Enum.map_join(mods, "\n", fn m -> "defmodule #{m} do\n  def hi, do: :ok\nend" end)

    manifest = """
    %{
      id: "#{id}",
      version: "#{version}",
      description: "test",
      author: "tester",
      modules: [#{Enum.join(mods, ", ")}],
      provides: [
        %{
          id: "main",
          provider: #{first_mod},
          subscriptions: [{:on_input, []}],
          default_opts: %{},
          scope: :operative
        }
      ],
      dependencies: #{inspect(deps)}
    }
    """

    checksums =
      "#{sha256(manifest)}  manifest.exs\n#{sha256(src_body)}  src/#{src_filename}\n"

    entries = [
      {"manifest.exs", manifest},
      {"src/#{src_filename}", src_body},
      {"CHECKSUMS.sha256", checksums}
    ]

    :ok = File.mkdir_p!(Path.dirname(fpz))

    :ok =
      :erl_tar.create(
        String.to_charlist(fpz),
        Enum.map(entries, fn {n, b} -> {String.to_charlist(n), b} end),
        [:compressed]
      )

    fpz
  end

  @doc "Lowercase hex SHA-256 of `binary`, the form `CHECKSUMS.sha256` carries."
  @spec sha256(binary()) :: binary()
  def sha256(binary), do: :crypto.hash(:sha256, binary) |> Base.encode16(case: :lower)

  @doc "Generate a fresh Ed25519 keypair suitable for signing tests."
  @spec generate_keypair() :: {public :: binary(), private :: binary()}
  def generate_keypair do
    :crypto.generate_key(:eddsa, :ed25519)
  end

  @doc "Sign a message with a private key; returns the 64-byte signature."
  @spec sign(binary(), binary()) :: binary()
  def sign(msg, private_key) do
    :crypto.sign(:eddsa, :none, msg, [private_key, :ed25519])
  end

  @doc "Is `module` currently loaded in the VM?"
  @spec loaded?(module()) :: boolean()
  def loaded?(module), do: :code.is_loaded(module) != false

  @doc "Is `plugin_id` registered with the Loader?"
  @spec registered?(binary()) :: boolean()
  def registered?(plugin_id) do
    case Process.whereis(Loader) do
      nil -> false
      _ -> MapSet.member?(Loader.installed_ids(), plugin_id)
    end
  end

  @doc "Return the modules the Loader considers owned by `plugin_id`."
  @spec modules_of(binary()) :: [module()]
  def modules_of(plugin_id) do
    case Process.whereis(Loader) do
      nil -> []
      _ -> Loader.modules_of(plugin_id)
    end
  end
end
