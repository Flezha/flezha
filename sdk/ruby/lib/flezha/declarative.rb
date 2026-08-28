# frozen_string_literal: true

module Flezha
  # The class-level declaration DSL the authoring surface shares.
  #
  # One method reads and writes: `clock` answers what was declared,
  # `clock :event_time` declares it. Each declaration registers its name, which
  # is what makes inheritance **total** — a subclass copies exactly the
  # settings that exist, rather than a list someone has to remember to extend
  # when they add one.
  #
  # That inheritance is the point. Without it a subclass silently loses every
  # declaration its parent made: its entity mode falls back to `:single`, its
  # mailbox capacity to the default, its schema version to 1 — and nothing
  # reports the difference until the topology is wrong at runtime.
  module Declarative
    # Every declaration name known to this class, its own and its ancestors'.
    def settings
      own = @settings ||= []
      superclass.respond_to?(:settings) ? superclass.settings | own : own
    end

    # Register names a hand-written declaration writes, so inheritance covers
    # them too. Used by the declarations that take more than one value, or that
    # write more than one instance variable.
    def declares(*names)
      (@settings ||= []).concat(names)
    end

    # A declaration read by its bare name and written by naming a value.
    def setting(*names)
      declares(*names)
      names.each do |name|
        singleton_class.define_method(name) do |value = nil|
          return instance_variable_get(:"@#{name}") if value.nil?

          instance_variable_set(:"@#{name}", value)
        end
      end
    end

    # A declaration whose bare mention declares it: `sync_durability`.
    def flag(name)
      declares(name)
      singleton_class.define_method(name) do |value = true|
        instance_variable_set(:"@#{name}", value)
      end
    end

    # A declaration that takes a block, read by its bare name.
    def block_setting(*names)
      declares(*names)
      names.each do |name|
        singleton_class.define_method(name) do |&block|
          return instance_variable_get(:"@#{name}") unless block

          instance_variable_set(:"@#{name}", block)
        end
      end
    end

    def inherited(subclass)
      super
      settings.each do |name|
        value = instance_variable_get(:"@#{name}")
        subclass.instance_variable_set(:"@#{name}", value) unless value.nil?
      end
    end
  end
end
