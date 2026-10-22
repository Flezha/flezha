defmodule FlezhaTest.Tiers do
  @moduledoc """
  One row per test tier: what tags it, where it runs, what it needs, which
  transports it covers and where it is documented.

  A tier is a slice of the suite that plain `mix test` does not run. Before this
  table a tier was edited into four places by hand: the exclude list in
  `test/test_helper.exs`, an `--include` line in a workflow, a paragraph in
  `docs/internal/sdk/conformance.md` and the tag itself. Three of the nine tiers never
  reached the document and six were run by no workflow at all, so a tier nothing
  runs looked exactly like a tier that runs nightly.

  `test/test_helper.exs` derives `exclude:` from `excluded/0`, and
  `test/sdk_conformance/tier_gate_test.exs` judges every other half.

  ## The fields

    * `:tag` - the ExUnit tag, and the token an `--include` names.
    * `:runs_in` - `:workflow` when some workflow runs it, `:manual` when a
      person does. `:manual` requires `:reason`.
    * `:workflow` - the file under `.github/workflows/` whose gate rows
      (`selected_rows/1`) name the tag.
    * `:reason` - why a `:manual` tier is not in CI. A tier with neither a
      workflow nor a reason is a gate failure, because it is a slice of the
      suite that nothing anywhere runs.
    * `:within` - the tier this one is a named subset of. A subset runs whenever
      its parent does, so it needs no `--include` line of its own; `:sdk_smoke`
      has one anyway because it is also the two-minute inner loop a person runs
      on its own.
    * `:excluded` - whether plain `mix test` excludes the tag. `:sdk_smoke` is
      the one tier that is not: its module is already `:system`, so the smoke
      tier is a selector inside an excluded tier rather than an exclusion of its
      own.
    * `:needs` - what a host must have before the tier can run.
    * `:transports` - the transports the tier covers.
    * `:doc` - the anchor in `docs/internal/sdk/conformance.md` that describes it.
    * `:self_skip` - the tier skips itself unless its own tag is included. An
      ExUnit include BEATS an exclude, so `--include system` would otherwise run
      `:interop` however it is excluded.

  `blocks_merge/1` is DERIVED, never declared: a tier blocks merge when the
  workflow that runs it has a `pull_request` trigger. That makes "this mechanism
  is proven only nightly" a value this table prints rather than something a
  reader reconstructs from an exclude list plus a workflow trigger.
  """

  @workflows "../../.github/workflows" |> Path.expand(__DIR__)

  @tiers [
    %{
      tag: :system,
      runs_in: :workflow,
      workflow: "nightly-e2e.yml",
      reason: nil,
      within: nil,
      excluded: true,
      needs: "an SDK runtime named by FLEZHA_E2E_RUNTIME, built as an OS process",
      transports: [:uds, :quic],
      doc: "The cross-SDK conformance suite",
      self_skip: false
    },
    %{
      tag: :sdk_smoke,
      runs_in: :workflow,
      workflow: "java-sdk.yml",
      reason: nil,
      within: :system,
      excluded: false,
      needs: "the same runtime as :system; it is the two-minute subset of it",
      transports: [:uds, :quic],
      doc: "The cross-SDK conformance suite",
      self_skip: false
    },
    %{
      tag: :steps_crash_resume,
      runs_in: :workflow,
      workflow: "nightly-e2e.yml",
      reason: nil,
      within: :system,
      excluded: false,
      needs: "the same runtime as :system; it is the crash-and-resume slice of it",
      transports: [:uds],
      doc: "The test tiers",
      self_skip: false
    },
    %{
      tag: :interop,
      runs_in: :workflow,
      workflow: "nightly-e2e.yml",
      reason: nil,
      within: nil,
      excluded: true,
      needs: "two SDK runtimes in one run, and a namespace no other tier is using",
      transports: [:uds, :quic],
      doc: "When a run is red",
      self_skip: true
    },
    %{
      tag: :multi_node,
      runs_in: :workflow,
      workflow: "nightly-e2e.yml",
      reason: nil,
      within: nil,
      excluded: true,
      needs: "a peer BEAM this tier boots and stops mid-suite",
      transports: [:uds],
      doc: "The test tiers",
      self_skip: true
    },
    %{
      tag: :testnet,
      runs_in: :manual,
      workflow: nil,
      reason:
        "the WebSocket testnet daemon serves a bundle `mix test.setup` builds from " <>
          "test/apps/testnet, which is npm install plus a build no CI job runs",
      within: nil,
      excluded: true,
      needs: "`mix test.setup`, then a free port Flezha.Testnet.ws_port/0 derives",
      transports: [:uds],
      doc: "The test tiers",
      self_skip: false
    },
    %{
      tag: :postgres_integration,
      runs_in: :manual,
      workflow: nil,
      reason: "the postgres compose service, which no CI job stands up",
      within: nil,
      excluded: true,
      needs: "the `postgres` compose service",
      transports: [:uds],
      doc: "The test tiers",
      self_skip: false
    },
    %{
      tag: :s3_integration,
      runs_in: :manual,
      workflow: nil,
      reason: "the minio compose service on localhost:9100, which no CI job stands up",
      within: nil,
      excluded: true,
      needs: "the `minio` compose service and S3_INTEGRATION=1",
      transports: [:uds],
      doc: "The test tiers",
      self_skip: false
    },
    %{
      tag: :memory_drift,
      runs_in: :manual,
      workflow: nil,
      reason: "a long-running load harness whose runtime is measured in hours",
      within: nil,
      excluded: true,
      needs: "MIX_RUN_MEMORY_DRIFT=1 and the time the harness takes",
      transports: [:uds],
      doc: "The test tiers",
      self_skip: false
    },
    %{
      tag: :credit_soak,
      runs_in: :manual,
      workflow: nil,
      reason: "a soak whose runtime is measured in hours",
      within: nil,
      excluded: true,
      needs: "the time the soak takes",
      transports: [:uds],
      doc: "The test tiers",
      self_skip: false
    }
  ]

  @doc "Every declared tier, in table order."
  @spec all() :: [map()]
  def all, do: @tiers

  @doc "Every declared tag."
  @spec tags() :: [atom()]
  def tags, do: Enum.map(@tiers, & &1.tag)

  @doc """
  The tags plain `mix test` excludes. `test/test_helper.exs` is the one reader,
  so a tier added here is excluded without a second edit.
  """
  @spec excluded() :: [atom()]
  def excluded, do: for(tier <- @tiers, tier.excluded, do: tier.tag)

  @doc "The row for `tag`, or `nil`."
  @spec fetch(atom()) :: map() | nil
  def fetch(tag), do: Enum.find(@tiers, &(&1.tag == tag))

  @doc """
  Whether a red run of `tag` stops a pull request: true when the workflow that
  runs it has a `pull_request` trigger. Derived from the workflow file, never
  declared, so a trigger change cannot leave this table saying the old thing.
  """
  @spec blocks_merge(atom() | map()) :: boolean()
  def blocks_merge(tag) when is_atom(tag), do: blocks_merge(fetch(tag))
  def blocks_merge(%{workflow: nil}), do: false

  def blocks_merge(%{workflow: file}) do
    @workflows
    |> Path.join(file)
    |> File.read!()
    |> triggers()
    |> Enum.member?("pull_request")
  end

  @doc "The `on:` keys of a workflow file, as strings."
  @spec triggers(String.t()) :: [String.t()]
  def triggers(source) do
    source
    |> String.split("\n")
    |> Enum.drop_while(&(&1 != "on:"))
    |> Enum.drop(1)
    |> Enum.take_while(&(&1 == "" or String.starts_with?(&1, " ")))
    |> Enum.flat_map(fn line ->
      case Regex.run(~r/^  ([a-z_]+):/, line) do
        [_, key] -> [key]
        nil -> []
      end
    end)
  end

  @doc """
  `nil` when `tag` is included in this run, else the reason a module of that
  tier must skip itself.

  An ExUnit include BEATS an exclude, so a tier that must not ride another one's
  include cannot be kept out by a filter at all: `--include system` would run
  `:interop` however it is excluded. A skip is honoured whatever the filters
  say, and this is the one place the reason is written.
  """
  @spec skip_unless_included(atom()) :: String.t() | nil
  def skip_unless_included(tag) do
    tier = fetch(tag)

    cond do
      tier == nil -> raise ArgumentError, "#{tag} is not a declared tier"
      not tier.self_skip -> nil
      tag in (ExUnit.configuration()[:include] || []) -> nil
      true -> "#{tag} needs #{tier.needs}; pass --include #{tag}"
    end
  end

  @doc """
  The commands of the gate rows a workflow runs. A workflow spells no `mix test`
  line of its own: it selects rows with `naming_gate.py --run <spec>`, itself or
  through a composite action of this repository it uses. A `${{ ... }}` in the
  spec is read as every runtime that has a manifest, and such a spec that names
  a group some runtime does not have adds nothing for it. Any other failure
  raises with the spec and naming_gate.py's output.
  """
  @spec selected_rows(String.t()) :: String.t()
  def selected_rows(source) do
    actions =
      for [_, name] <- Regex.scan(~r{uses: \./\.github/actions/([\w-]+)}, source), uniq: true do
        File.read!(Path.join([@workflows, "..", "actions", name, "action.yml"]))
      end

    for [_, spec] <-
          Regex.scan(
            ~r/naming_gate\.py[\s\\]+--run\s+((?:\$\{\{[^}]*\}\}|\S)+)/,
            Enum.join([source | actions], "\n")
          ),
        runtime <- Flezha.Service.PortPilot.runtimes(),
        uniq: true do
      expanded = Regex.replace(~r/\$\{\{[^}]*\}\}/, spec, Atom.to_string(runtime))
      {expanded, expanded != spec}
    end
    |> then(fn specs ->
      # One listing for every spec: a workflow can name dozens of specs.
      groups = for line <- gate_list("all"), [_id, group | _] <- [String.split(line)], do: group

      specs
      |> Enum.reject(fn {spec, expanded?} -> expanded? and missing_group?(spec, groups) end)
      |> Enum.map_join(",", &elem(&1, 0))
      |> case do
        "" -> ""
        joined -> Enum.join(gate_list(joined), "\n")
      end
    end)
  end

  defp missing_group?(spec, groups),
    do:
      Enum.any?(
        String.split(spec, ","),
        &(String.starts_with?(&1, "@") and String.trim_leading(&1, "@") not in groups)
      )

  # Kept for the test run: every tier a workflow runs lists the same specs.
  defp gate_list(spec) do
    key = {__MODULE__, :gate_list, spec}

    case :persistent_term.get(key, nil) do
      nil -> tap(list_gates!(spec), &:persistent_term.put(key, &1))
      rows -> rows
    end
  end

  defp list_gates!(spec) do
    case System.cmd("python3", ["scripts/naming_gate.py", "--list", spec], stderr_to_stdout: true) do
      {rows, 0} -> String.split(rows, "\n", trim: true)
      {output, _} -> raise "naming_gate.py --list #{spec} failed:\n#{output}"
    end
  end

  @doc "The workflow directory this table reads its triggers from."
  @spec workflow_dir() :: Path.t()
  def workflow_dir, do: @workflows
end
