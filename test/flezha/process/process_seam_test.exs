defmodule Flezha.Process.SeamTest do
  @moduledoc """
  The five process mechanisms, and the rule that a site declares into them
  rather than re-implementing them.

  Clauses 1-5 are the bypass gate: one shape per mechanism, read from the
  tree, with the seam's own files as the only place the primitive may appear.
  Each is red on a scratch bypass — a `Process.monitor/1` in a module keeping
  its own ref map, a literal child-spec map, a hand-written timer helper —
  and the failure names the file.

  Clause 6 is the declaration test: every declared thing exists and every
  existing thing is declared, so adding a resolver, a restart kind or a timer
  key without its row is red with no test edit.
  """

  use ExUnit.Case, async: true

  alias Flezha.Relay.Lookup
  alias Flezha.Process.Cspec
  alias Flezha.Process.Timer
  alias FlezhaTest.Helper.SourceWalk

  @seam_files [
    "lib/flezha/process/parallel.ex",
    "lib/flezha/process/watch.ex",
    "lib/flezha/process/timer.ex",
    "lib/flezha/process/cspec.ex",
    "lib/flezha/process/hub.ex",
    "lib/flezha/relay/lookup.ex",
    "lib/flezha/relay/lookup/class.ex",
    "lib/flezha/relay/lookup/hub.ex"
  ]

  # A plugin package's processes keep timers too, so the timer clauses read it.
  defp timer_roots, do: ["lib/flezha" | Path.wildcard("plugins/*/lib")]

  defp sources(roots) do
    roots
    |> SourceWalk.files([".ex"])
    |> Enum.reject(&(&1 in @seam_files))
  end

  # `SourceWalk` holds the walk and the comment strip for the run: six clauses
  # asking six questions of one tree is one read, not six. A file whose text
  # never matches is not split into lines.
  defp hits(pattern, allow \\ [], roots \\ ["lib/flezha"]) do
    for path <- sources(roots),
        path not in allow,
        Regex.match?(pattern, SourceWalk.read(path)),
        {_path, line, n} <- SourceWalk.code_lines([path]),
        Regex.match?(pattern, line),
        do: "#{path}:#{n}"
  end

  # ---------- clause 1: a death is bookkept by `Watch`, not by a private ref map ----------

  test "no module outside the seam takes a monitor of its own" do
    # fails when: a module calls `Process.monitor/1` and keeps the ref in a map
    # of its own, which is the bookkeeping `Watch` exists to hold — one monitor
    # per pid however many tags, and `demonitor(:flush)` on the last one.
    found = hits(~r/Process\.monitor\(/, monitor_allow())
    assert found == [], "a monitor outside `Flezha.Process.Watch`: #{Enum.join(found, ", ")}"
  end

  # A monitor whose ref IS the value — a published subscription handle, a
  # protocol token another process is told, a single owner pid — is not
  # bookkeeping and folding it would hide the ref its own callers name.
  defp monitor_allow do
    [
      # the ref is returned to the caller as its subscription handle
      "lib/flezha/observability/audit_log/writer.ex",
      # one monitor for one owner; no map, nothing to book
      "lib/flezha/cli/socket.ex",
      "lib/flezha/plugin/installer.ex",
      "lib/flezha/service/hub_manager/lifecycle.ex",
      # the ref is a field of the worker entry the table replaces in place
      "lib/flezha/plugin/extension/table/workers.ex",
      # one monitor for one owner, held in a named field, with no map to book
      "lib/flezha/plugin/provider/http_poll/poll_worker.ex",
      "lib/flezha/plugin/provider/relay_forward/forward_worker.ex",
      # a synchronous wait's own monitor, as `GenServer.call` takes one
      "lib/flezha/relay/calls/inbound.ex",
      "lib/flezha/state/input_log/syncer.ex",
      "lib/flezha/state/writer_registry.ex",
      "lib/flezha/authz/caller_context.ex"
    ]
  end

  # ---------- clause 2: a timer is declared, not hand-rolled ----------

  test "no module outside the seam arms a timer without declaring it" do
    # fails when: a module calls `Process.send_after/3` or `Process.cancel_timer/1`
    # without `use Flezha.Process.Timer` — a timer nobody wrote down, with its
    # own field name, its own cancel guard and its own stale-fire hazard.
    raw = hits(~r/Process\.(send_after|cancel_timer)\(/, timer_allow(), timer_roots())

    undeclared =
      for path <- raw |> Enum.map(&file_of/1) |> Enum.uniq(),
          not String.contains?(SourceWalk.read(path), "use Flezha.Process.Timer"),
          do: path

    assert undeclared == [], "a timer armed outside `Flezha.Process.Timer`"
  end

  # A timer whose reference IS a protocol value — one a caller is told, one
  # keyed by the item it belongs to inside a structure that owns it — is not a
  # process's own named timer, and declaring it would put the bookkeeping in
  # two places rather than one.
  defp timer_allow do
    [
      # a per-relay retry send-later from a pure helper; nothing is kept
      "lib/flezha/runner/control_channel/commits.ex",
      # a table of per-subscription timers across plugins, keyed by ref so the
      # host's `:DOWN` can find the record the timer belongs to; `health.ex`
      # holds the cancel the table's teardown calls over that same table
      "lib/flezha/plugin/extension/table/subscription.ex",
      "lib/flezha/plugin/extension/table/health.ex",
      # a per-batch dispatch timer held in the batch, a per-caller block timer
      # whose message names the caller, and the linger timer read per record
      "plugins/flezha_kafka_plugin/lib/flezha_kafka/producer_worker/window.ex",
      # a retry send-later that keeps nothing; its message carries the
      # registry ref or attempt that makes a stale one harmless
      "plugins/flezha_kafka_plugin/lib/flezha_kafka/producer_worker/client.ex",
      "plugins/flezha_kafka_plugin/lib/flezha_kafka/producer_worker.ex"
    ]
  end

  defp file_of(hit), do: hit |> String.split(":") |> hd()

  # ---------- clause 3: a supervised child declares a kind ----------

  test "no module outside the seam writes a literal child-spec map" do
    # fails when: a module writes `%{id: …, start: {…}}` by hand, repeating the
    # six keys a restart kind already implies.
    assert hits(~r/start: \{[A-Z_:]/, cspec_allow()) == [],
           "a child spec written by hand rather than built from a kind"
  end

  defp cspec_allow do
    [
      # a cspec ProcessHub persists and reads back after a restart: its shape is
      # durable state and this change only READS one
      "lib/flezha/service/relay_manager/init.ex",
      "lib/flezha/schedule/job.ex",
      "lib/flezha/service/hub_manager/rows.ex",
      "lib/flezha/service/hub_manager/placement.ex",
      "lib/flezha/state/recovery.ex"
    ]
  end

  # ---------- clause 4: one answerer of "which process serves this relay" ----------

  test "the class registry's pid read has one caller, and it is the resolver" do
    # fails when: a second module answers "relay → pid" for itself, which is
    # how five answerers and three spellings of one failure grew.
    assert hits(~r/ClassRegistry\.overwatch_pid\(/, [
             "lib/flezha/relay/class_registry.ex"
           ]) == [],
           "a second answerer of `relay → pid`"
  end

  test "the hub's liveness guard has one implementation" do
    # fails when: a module re-writes the `is_alive?` + catch pair, which answers
    # `true` for a terminating coordinator and must survive the hub going away
    # mid-call.
    assert hits(~r/ProcessHub\.is_alive\?\(/) == [],
           "a hub liveness check outside `Flezha.Process.Hub`"
  end

  # ---------- clause 5: a fan-out takes its bound from the seam ----------

  test "no module outside the seam builds its own concurrent pipeline" do
    # fails when: a caller needing a per-item bound builds `Task.async_stream`
    # itself, because the seam could not take one.
    assert hits(~r/Task\.async_stream\(/) == [], "a fan-out outside `Flezha.Parallel`"
  end

  # ---------- clause 6: declaration ↔ tree ----------

  test "every resolver is declared and every declared resolver resolves" do
    for resolver <- Lookup.resolvers() do
      assert Code.ensure_loaded?(resolver)
      assert function_exported?(resolver, :resolve, 1)
      assert function_exported?(resolver, :key_kind, 0)
      assert resolver.key_kind() in Lookup.kinds()
    end

    implementers =
      for path <- Path.wildcard("lib/flezha/relay/lookup/*.ex"),
          src = File.read!(path),
          String.contains?(src, "@behaviour Flezha.Relay.Lookup.Resolver"),
          [_, mod] = Regex.run(~r/defmodule ([\w.]+) do/, src),
          do: Module.concat([mod])

    assert Enum.sort(implementers) == Enum.sort(Lookup.resolvers())
  end

  test "every restart kind the tree uses is a row of the policy table" do
    # The kind is the argument after the `{m, f, a}` tuple, so the scan anchors
    # on the tuple's closing bracket rather than on a comma.
    used =
      for path <- SourceWalk.files(["lib/flezha"], [".ex"]),
          [_, kind] <- Regex.scan(~r/Cspec\.build\(.*?\]\},\s*:(\w+)/s, SourceWalk.read(path)),
          do: String.to_atom(kind)

    assert used != [], "no site builds a child spec — the shape moved"
    assert Enum.uniq(used) -- Cspec.kinds() == []
  end

  test "every armed timer key is declared by its module" do
    for path <- SourceWalk.files(timer_roots(), [".ex"]),
        src = SourceWalk.read(path),
        [_, declared] <- Regex.scan(~r/use Flezha\.Process\.Timer, keys: \[([^\]]*)\]/, src) do
      keys = declared |> String.split(",") |> Enum.map(&String.trim/1)

      armed =
        for [_, k] <- Regex.scan(~r/arm_timer\([^,]+,\s*:(\w+),/, src), do: ":#{k}"

      assert Enum.uniq(armed) -- keys == [], "#{path} arms a key it does not declare"
    end
  end

  test "a declared key names a family, so a per-item timer is declared too" do
    timers = Timer.arm(Timer.new(), {:reconnect, :node@host}, 50, :ping)
    assert Timer.armed?(timers, {:reconnect, :node@host})
    refute Timer.armed?(timers, :reconnect)
  end
end
