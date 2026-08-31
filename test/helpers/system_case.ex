defmodule FlezhaTest.SystemCase do
  @moduledoc """
  The case template every `test/system` module uses:

      use FlezhaTest.SystemCase, capability: :retry_ladder

  It declares the module `:system` (so plain `mix test` spawns no runner), tags
  it with the capability its scenarios prove — the axis the selected SDK
  runtime's manifest defers from — and points its runner spawns at that
  runtime. Only these modules follow `FLEZHA_E2E_RUNTIME`: a controller test
  that merely needs *a* runner keeps the reference SDK, because it is written
  against the reference fixtures rather than against the conformance
  vocabulary.

  A deferred capability rides `:skip`, not an ExUnit filter: `--include system`
  is what runs this suite, and an include overrides an exclude, so a filter
  would run the very tests the manifest says the SDK does not carry. A skip is
  honoured whatever the filters say, and reports the capability by name.

  A scenario that proves controller mechanics rather than the runner pins the
  reference SDK with `@tag reference_runtime: true`, and says why.

  A module whose scenarios span two capabilities overrides per test:

      @tag capability: :effectively_once, skip: FlezhaTest.SystemCase.deferral(:effectively_once)

  Transports are the other axis and need no skip — a test enumerates
  `FlezhaTest.Helper.NodeTestHelper.transports/0`, so a transport the runtime
  does not support never becomes a test.
  """

  alias FlezhaTest.Helper.NodeTestHelper

  @doc """
  The reason `capability` is skipped for the selected runtime, or `nil` when it
  is carried — `nil` being what makes `skip:` inert.
  """
  @spec deferral(atom()) :: String.t() | nil
  def deferral(capability) do
    runtime = NodeTestHelper.runtime()

    if capability in NodeTestHelper.deferred(runtime) do
      "#{capability} is deferred by the #{runtime} SDK (#{NodeTestHelper.manifest_path(runtime)})"
    end
  end

  @doc """
  `nil` when `condition` holds, else `reason` — the `skip:` value of a module
  that must not run in the ordinary `--include system` pass: an ExUnit include
  beats an exclude, so a tag alone cannot keep it out, while a skip is honoured
  whatever the filters say.
  """
  @spec skip_unless(boolean(), String.t()) :: String.t() | nil
  def skip_unless(condition, reason), do: if(condition, do: nil, else: reason)

  @doc """
  The runtime a scenario spawns: the selected SDK, or the reference when the
  scenario is tagged `reference_runtime: true`.

  Not every `test/system` scenario proves the runner. Some prove *controller*
  mechanics — a journal cut, an outbox re-drive, a rewind — and merely need a
  runner to drive them. Those name the reference SDK so a run pointed at
  another one neither re-proves controller behaviour nor reports a runner gap
  that is not what the scenario is about. Each one says why in a comment.
  """
  def runtime_for(%{reference_runtime: true}),
    do: NodeTestHelper.reference_runtime()

  def runtime_for(_context), do: NodeTestHelper.runtime()

  defmacro __using__(opts) do
    capability = Keyword.fetch!(opts, :capability)

    quote do
      # async: false: every scenario drives the live controller and its fleet.
      use ExUnit.Case, async: false

      @moduletag :system
      @moduletag capability: unquote(capability)
      @moduletag skip: FlezhaTest.SystemCase.deferral(unquote(capability))

      setup context do
        NodeTestHelper.put_spawn_runtime(FlezhaTest.SystemCase.runtime_for(context))

        :ok
      end
    end
  end
end
