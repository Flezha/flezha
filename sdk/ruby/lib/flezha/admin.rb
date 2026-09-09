# frozen_string_literal: true

require "json"

module Flezha
  # Administering the namespace this runner's connection proved, from ordinary
  # application code.
  #
  # No procedure has to be running and the application holds no credential of
  # its own: the controller judges every call as the identity the connection
  # already established, so nothing an unattended process holds expires. An
  # operator grants a namespace's applications the authority once and revokes
  # it the same way, and a revocation takes effect on the next call:
  #
  #     flz role bind runner@team-orders team-orders app
  #
  # Every method here is one line over a single native entry point, because the
  # verb travels as data. Adding a verb costs a method and no native work.
  class Admin
    def initialize(core)
      @core = core
    end

    # Start an operative, on the specification `flz operative start` takes.
    def start_operative(operative_id, factory_id, kind: nil, init_values: nil, opts: nil)
      fields = { "id" => operative_id, "factory_id" => factory_id }
      fields["kind"] = kind if kind
      fields["init_values"] = init_values if init_values
      fields["opts"] = opts if opts
      call("Operative.start", fields)
    end

    # Stop an operative, or the whole partition group it names.
    def stop_operative(operative_id) = call("Operative.stop", operative_id)

    # Inject a message into an operative's input stream, blocking for the verdict.
    def send_to(operative_id, message, channel: "")
      call("Operative.send", operative_id, message, channel)
    end

    # Enable a plugin, built from the fields `flz plugin enable` takes.
    def enable_plugin(plugin_id, fields) = call("PluginManager.enable", plugin_id, fields)

    # Disable a plugin, or every partitioned instance of it.
    def disable_plugin(plugin_id) = call("PluginManager.disable", plugin_id)

    # The operative ids running in this namespace.
    def list_operatives = call("Operative.list_operatives")

    # The plugins enabled on the controller node answering this call.
    def list_plugins = call("PluginManager.list")

    # One entity's live state, and the change sequence it was read at.
    def entity_state(operative_id, entity_key)
      call("Operative.entity_state", operative_id, entity_key)
    end

    # Any published function, by name. What succeeds follows from the role the
    # namespace is bound to, never from a list kept here. The arguments are the
    # call's own other than the namespace, which the controller fills from the
    # connection.
    def call(verb, *args)
      JSON.parse(@core.admin_call(verb, JSON.generate(args)))
    end
  end
end
