# frozen_string_literal: true

# The conformance run: every row of `test/fixtures/reference_topologies.txt`
# driven through THIS SDK's builder.
#
# `flz_core/src/host/check_tests.rs` is the same table driven against the
# record directly, because the core has no builder. This one maps the same step
# vocabulary onto the Ruby graph builder, so what is measured here is what an
# author of a Ruby operative actually writes.
#
# Two entry points, both off `runner.rb`:
#
#   --conformance-report <path>  drive every row and write the report the drill
#                                reads (test/system/topology_refusal_e2e_test.exs),
#                                then exit without connecting to a controller
#   FLEZHA_REFUSAL_FIXTURE=<id>  register ONLY that row's topology at startup,
#                                so a refused one kills the process with the
#                                refusal's own name in its output
#
# The palette procedures live here rather than in `fixtures.rb`: they exist
# only for this run, and nothing registers them but a row that names them.

require "json"

require "flezha"
require_relative "fixtures"
require_relative "fixtures_workflow"

module Conformance
  # The fixture, relative to the repository root the app is launched from.
  FIXTURE = "test/fixtures/reference_topologies.txt"

  # The authoring surfaces this builder carries — the same list the app's
  # `conformance.json` declares. A row needing anything else is skipped BY
  # NAME, which is what makes the skip checkable. There is no merge terminator
  # in this SDK; the class flag a typed builder cannot spell is a plain builder
  # call here.
  SURFACES = %w[branch workflow sync_durability].freeze

  # The two topologies the fixture cites rather than re-spells: this app's own
  # fixtures, the twins of its seven-column table.
  CITED = {
    "hybrid_channel" => Fixtures::HybridChannelOperative,
    "commit_block_chain" => Fixtures::TierCommitBlockWorkflow
  }.freeze

  # The palette. One fixture procedure per role the fixture header names; the
  # facts a role declares are the facts a rule reads. A row mints its own
  # subclass of each, because a Ruby procedure's mailbox capacity is a class
  # declaration and a resolved definition is memoised per class.
  class PaletteProcedure < Flezha::Procedure
    def process(input, _ctx) = input
  end

  class Plain < PaletteProcedure; end
  class Second < PaletteProcedure; end
  class Twin < PaletteProcedure; end

  class StaticKeys < PaletteProcedure
    entity_keys "a", "b"
  end

  class Routed < PaletteProcedure
    entity_keys "a", "b"
    def entities_for(_input) = ["a"]
  end

  class Dynamic < PaletteProcedure
    dynamic_entities
  end

  class NoChangeLogical < PaletteProcedure
    clock :logical
  end

  class EventTime < PaletteProcedure
    clock :event_time
    def event_time(input) = input.to_s.length
  end

  # role => [the class it is bound to, the node identity it declares]. `twin`
  # is a distinct class declaring the SAME name as `plain`: that IS the
  # collision.
  PALETTE = {
    "plain" => [Plain, "plain"],
    "second" => [Second, "second"],
    "twin" => [Twin, "plain"],
    "static" => [StaticKeys, "static"],
    "routed" => [Routed, "routed"],
    "dynamic" => [Dynamic, "dynamic"],
    "nochange" => [NoChangeLogical, "nochange"],
    "eventtime" => [EventTime, "eventtime"]
  }.freeze

  # `verb(arg)` -> ["verb", "arg"]; a bare word -> ["word", ""].
  def self.split_call(text)
    verb, arg = text.split("(", 2)
    [verb.strip, arg.to_s.chomp(")").strip]
  end

  # The record a row's steps describe, written the way an author writes it.
  class Interpreter
    # The links that leave a channel option pending: each answers a new edge
    # and nothing applies it until the next terminator, so the handle they were
    # declared off has to become that edge.
    KNOBS = %w[keyed retry_max on_full named].freeze

    def initialize(steps)
      @steps = steps.split(";").map(&:strip).reject(&:empty?)
      # A handle is a channel id from an `entry(...)` step, or a procedure role
      # naming that procedure's output channel.
      @handles = {}
      @classes = {}
      @mailbox = mailbox_capacities
    end

    def declare(builder)
      @steps.each { |step| run_step(builder, step) }
    end

    private

    def run_step(builder, step)
      verb, arg = Conformance.split_call(step)
      case verb
      when "entry" then @handles[arg] = builder.input(arg)
      when "branch" then branch(arg)
      when "merge"
        raise "this SDK carries no merge terminator; the row needs the `merge` surface"
      else dotted(builder, step)
      end
    end

    # `<handle>.<link>…` — a chain declared off a handle, a channel knob, a
    # procedure knob, or a class flag.
    def dotted(builder, step)
      head, rest = step.split(".", 2)
      return class_flag(builder, rest) if head == "class"

      links = rest.split(".")
      edge = links.reduce(handle(head)) { |current, link| apply_link(current, link) }
      @handles[head] = edge if KNOBS.include?(Conformance.split_call(links.last).first)
    end

    def apply_link(edge, link)
      verb, arg = Conformance.split_call(link)
      case verb
      when "map" then edge.map { |value| value }
      when "key" then edge.key_by(&:to_s)
      when "named" then edge.named(arg)
      when "keyed" then edge.keyed
      when "retry_max" then edge.retry_max(Integer(arg))
      when "on_full" then edge.on_full(arg.to_sym)
      when "sink" then arg.empty? ? edge.sink : edge.sink(arg)
      when "into" then wire(edge, arg)
      # Declared on the procedure in this SDK, and read as its class was
      # minted — one step earlier than the step that names it.
      when "mailbox" then edge
      else raise "the fixture spells a link this interpreter does not map: #{verb}"
      end
    end

    # `<x>.into(<role>)` — the palette role bound to its fixture procedure. The
    # result is that procedure's output channel, addressed by its role.
    def wire(edge, role)
      @handles[role] = edge.into(procedure_class(role))
    end

    # `branch(<x>, <n>)` — the branch sugar's arity. The arms are empty: what
    # the row states is how many there are.
    def branch(arg)
      target, arms = arg.split(",").map(&:strip)
      handle(target).branch(*Array.new(Integer(arms)) { ->(_arm) {} })
    end

    # `class.<flag>[(<value>)]` — the facts the class declares on the builder.
    # `persistent` is an operative-level declaration, read as the row's class
    # was minted; `workflow` is a class KIND no stream row can carry, and every
    # row needing one is skipped before it reaches here.
    def class_flag(builder, rest)
      flag, value = Conformance.split_call(rest)
      case flag
      when "persistent" then nil
      when "effectively_once" then builder.effectively_once
      when "checkpoint_ack" then builder.checkpoint_ack
      when "checkpoint_every" then builder.checkpoint_every(Integer(value))
      when "correction_window" then builder.correction_window(Integer(value))
      when "outputs_at_checkpoint" then builder.outputs_at_checkpoint(Integer(value))
      when "sync_durability" then builder.sync_durability
      else raise "the fixture spells a class flag this interpreter does not map: #{flag}"
      end
    end

    def handle(name)
      @handles.fetch(name) { raise "the row addresses `#{name}`, which nothing has declared" }
    end

    # The row's own subclass of the role's palette procedure, minted once. The
    # identity is re-declared because a node identity is deliberately not
    # inherited, and the capacity because a definition is resolved once.
    def procedure_class(role)
      @classes[role] ||= begin
        base, identity = PALETTE.fetch(role) do
          raise "the fixture names a palette role that is not one: #{role}"
        end
        klass = Class.new(base)
        klass.procedure_name(identity)
        capacity = @mailbox[role]
        klass.mailbox_capacity(capacity) if capacity
        klass
      end
    end

    # The capacities a row declares, read BEFORE the steps run: a procedure is
    # minted by the `into` that first names it, which is one step earlier than
    # the `mailbox` step that declares its capacity.
    def mailbox_capacities
      @steps.filter_map do |step|
        role, rest = step.split(".", 2)
        next unless rest&.start_with?("mailbox(")

        [role, Integer(Conformance.split_call(rest).last)]
      end.to_h
    end
  end

  # The refusal table, in file order.
  def self.rows
    File.read(fixture_path).each_line.filter_map do |line|
      line = line.strip
      next if line.empty? || line.start_with?("#")

      cols = line.split("|").map(&:strip)
      next unless cols.size == 4

      { refusal: cols[0], topology: cols[1], steps: cols[2], needs: cols[3] }
    end
  end

  # The fixture as seen from wherever the app was launched: the drill runs it
  # from the repository root, a developer from the app's own directory.
  def self.fixture_path
    base = "."
    5.times do
      candidate = File.join(base, FIXTURE)
      return candidate if File.exist?(candidate)

      base = File.join(base, "..")
    end
    raise "#{FIXTURE} is not under any parent of the working directory"
  end

  # A row's operative class. Fresh per row: an operative memoises its own spec,
  # and `persistent` is a class declaration rather than a builder call.
  def self.operative_for(row)
    interpreter = Interpreter.new(row[:steps])
    klass = Class.new(Flezha::StreamOperative)
    klass.factory_id(row[:topology])
    klass.persistent(row[:steps].include?("class.persistent"))
    klass.channels { |builder| interpreter.declare(builder) }
    klass
  end

  def self.class_for(row)
    CITED[row[:topology]] || operative_for(row)
  end

  # One row's verdict, in the report's shape. The refusal NAMES are the core's
  # own, carried on the refusal this SDK raises; nothing here re-derives one.
  def self.judge(row, core)
    if row[:needs] != "-" && !SURFACES.include?(row[:needs])
      return { "topology" => row[:topology], "outcome" => "skipped", "needs" => row[:needs] }
    end

    derived = Flezha::Graph.validate(class_for(row).spec, core)
    { "topology" => row[:topology], "outcome" => "accepted", "derived" => channels_of(derived) }
  rescue Flezha::DeclarationError => e
    { "topology" => row[:topology], "outcome" => "refused", "names" => e.refusals }
  end

  # The derived declaration in the columns the fixture's seven-column table
  # states. The core spells a consumer `{"procedure":N,"chain":M}`; the report
  # carries the flat list of procedure indices, which is what that table pins.
  def self.channels_of(derived)
    derived["channels"].map do |channel|
      {
        "index" => channel["index"],
        "name" => channel["name"],
        "kind" => channel["kind"],
        "producer" => channel["producer"],
        "consumers" => channel["consumers"].map { |edge| edge["procedure"] },
        "sink" => channel["sink"]
      }
    end
  end

  # Drive the whole table and write the report. Nothing here connects to a
  # controller: the run is the builder's answer to a file.
  def self.write_report(path)
    core = Flezha::Core.create
    table = rows
    judged = table.map { |row| judge(row, core) }
    File.write(path, JSON.pretty_generate({ "runtime" => "ruby", "rows" => judged }))
    skipped = judged.count { |entry| entry["outcome"] == "skipped" }
    warn("conformance: #{table.size} rows, #{table.size - skipped} ran, #{skipped} skipped")
  ensure
    core&.free
  end

  # Register ONLY the named row's topology, the way the app registers any
  # class. A refused row never reaches a connected runner: the declaration is
  # judged before the core is started, and the process dies naming the refusal.
  def self.register_only(runner, topology)
    row = rows.find { |candidate| candidate[:topology] == topology }
    raise "#{FIXTURE} has no row `#{topology}`" unless row

    runner.define_stream(class_for(row))
  end
end
