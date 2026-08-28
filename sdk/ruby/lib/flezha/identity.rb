# frozen_string_literal: true

module Flezha
  # Entity identity bytes and the replay-stable identifier derived from them.
  #
  # Both algorithms are fixed by the SDK contract rather than by a library, so
  # every SDK derives the same value from the same entity: the identity is
  # length-prefixed `instance | procedure | key`, and the identifier is FNV-1a
  # over `identity | change seq | per-invocation ordinal`.
  module Identity
    FNV128_OFFSET_BASIS = 0x6c62272e07bb014262b821756295c58d
    FNV128_PRIME = 0x0000000001000000000000000000013b
    MASK128 = (1 << 128) - 1

    module_function

    def fnv1a128(seed, bytes)
      hash = seed
      bytes.each_byte do |byte|
        hash = (hash ^ byte) & MASK128
        hash = (hash * FNV128_PRIME) & MASK128
      end
      hash
    end

    # `[len instance][instance][len procedure][procedure][u16 len key][key]`,
    # big-endian lengths. The same layout every SDK writes, so an identifier
    # derived here matches one derived there for the same entity.
    def entity_identity(instance_id, procedure_name, entity_key)
      instance = instance_id.to_s.b
      procedure = procedure_name.to_s.b
      key = entity_key.to_s.b
      if instance.bytesize > 255 || procedure.bytesize > 255
        raise Error, "operative class / procedure id must be at most 255 bytes"
      end
      raise Error, "entity key must be at most 65535 bytes" if key.bytesize > 0xffff

      [instance.bytesize, instance, procedure.bytesize, procedure, key.bytesize, key]
        .pack("Ca*Ca*na*")
    end

    # Thirty-two lowercase hex characters. Repeated calls in one invocation
    # differ; the n-th call of a replayed invocation equals the n-th call of the
    # original, which is what makes it the replacement for a random UUID.
    def stable_id(identity, change_seq, ordinal)
      hash = fnv1a128(FNV128_OFFSET_BASIS, identity)
      hash = fnv1a128(hash, [change_seq & 0xffffffffffffffff].pack("Q>"))
      hash = fnv1a128(hash, [ordinal & 0xffffffff].pack("N"))
      format("%032x", hash)
    end
  end
end
