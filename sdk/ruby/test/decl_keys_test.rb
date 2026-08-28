# frozen_string_literal: true

require_relative "test_helper"

# Every `HostClassDecl` key the core learns, spelled by this builder.
#
# The fixture is the list of record, shared with the `flz_core` test that
# asserts the core's own struct and with the Java and Node builders' tests. A
# key the core learns that Ruby cannot spell fails here, which is the point:
# the cross-SDK rule is that a declaration key lands in every SDK in one
# change, and this is what makes forgetting one a red build rather than a
# silently narrower SDK.
class DeclKeysTest < Minitest::Test
  KEYS_FILE = File.join(Flezha::TestSupport.repo_root, "test/fixtures/host_class_decl_keys.txt")

  # A declaration populated far enough that every optional key is present.
  class Entry < Flezha::Procedure
    procedure_name "entry"
    clock :event_time
    mailbox_capacity 8
    schema_version 3
    min_readable_schema_version 1
    migrate_state { |_from, bytes| Flezha::Codec.decode(bytes) }
    def initial_state(_key) = { seen: [] }
    def apply_change(state, change) = state[:seen] << change[:push]
    def event_time(input) = Integer(input)
    def process(input, ctx) = ctx.change({ push: input })
  end

  class Downstream < Flezha::Procedure
    procedure_name "downstream"
    dynamic_entities idle_ttl_ms: 1_000, max_entities: 16
    def process(input, _ctx) = input
    def compensate(_outputs, _ctx) = nil
  end

  class Fanned < Flezha::Procedure
    procedure_name "fanned"
    entity_keys "a", "b"
    def entities_for(_input) = ["a"]
    def process(input, _ctx) = input
  end

  class FullStream < Flezha::StreamOperative
    factory_id "decl_keys_stream"
    channels do |c|
      c.effectively_once
      c.checkpoint_every(4)
      c.outputs_at_checkpoint(250)
      c.correction_window(5_000)
      c.beyond_window_policy(:drop)
      c.compact_after_bytes(4_096)
      edge = c.input("input").into(Entry)
      edge.named("hop").key_by { |value| value.to_s }.retry_max(5).retry_backoff_ms(50)
          .into(Downstream)
      edge.named("fan").into(Fanned)
      c.input("second").into(Entry).named("sink").keyed.sink("out")
    end
  end

  class FullWorkflow < Flezha::WorkflowOperative
    factory_id "decl_keys_workflow"
    loop_mode :chained
    rollback_mode :keep_progress
    retry_attempts 4
    retry_backoff_ms 100
    barrier_retry_attempts 2
    max_duration_ms 60_000
    wake_after_ms 5_000
    sync_durability
    run { |args, _ctx| args }
  end

  def spelled
    stream = Flezha::Graph.to_host_class_decl(FullStream.spec)
    workflow = Flezha::Graph.to_host_class_decl(FullWorkflow.spec)
    keys = stream.keys.map { |key| "class.#{key}" }
    keys |= workflow.keys.map { |key| "class.#{key}" }
    keys |= stream["channels"].flat_map { |c| c.keys.map { |key| "channel.#{key}" } }
    keys |= stream["procedures"].flat_map { |p| p.keys.map { |key| "procedure.#{key}" } }
    keys |= workflow["workflow"].keys.map { |key| "workflow.#{key}" }
    keys.sort
  end

  def declared
    File.readlines(KEYS_FILE, chomp: true)
        .reject { |line| line.empty? || line.start_with?("#") }
        .sort
  end

  def test_the_builder_spells_every_declared_key
    missing = declared - spelled
    assert_empty missing,
                 "the Ruby builder cannot spell #{missing.join(', ')}; teach it, or the core " \
                 "learned a key this SDK will never send"
  end

  def test_the_builder_invents_no_key_the_core_never_learned
    # `deny_unknown_fields` would refuse one at declare time, but at the first
    # controller connection rather than here.
    extra = spelled - declared
    assert_empty extra, "the Ruby builder emits #{extra.join(', ')}, which the core never learned"
  end
end
