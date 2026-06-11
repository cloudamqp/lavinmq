require "amq-protocol"

# Vendored from amq-protocol.cr (proposed upstream as Table#has_entry?,
# commit 922bc49 on branch table-has-entry).
# TODO: Remove this file once a release of amq-protocol that includes
# Table#has_entry? is required by shard.yml.
module AMQ
  module Protocol
    class Table
      # Returns true if the table contains *key* and its value equals *value*.
      # Semantically identical to `has_key?(key) && self[key] == value` but
      # does not allocate for scalar and string values.
      def has_entry?(key : String, value : Field) : Bool
        with_pos_loop do |pos|
          if key_matches?(pos, key.to_slice)
            return field_equals?(pos, value)
          else
            skip_field(pos)
          end
        end
        false
      end

      # Compares the field at *pos* with *value* without materializing a
      # String when both are strings; other types fall back to `read_field`.
      private def field_equals?(pos : Int32*, value : Field) : Bool
        if value.is_a?(String)
          ensure_available(pos.value, 1)
          if 'S' === @buffer[pos.value]
            pos.value += 1
            sz = BYTEFORMAT.decode(UInt32, to_slice(pos.value, sizeof(UInt32)))
            pos.value += sizeof(UInt32)
            stored = to_slice(pos.value, sz)
            pos.value += sz
            return value.to_slice == stored
          end
        end
        read_field(pos) == value
      end
    end
  end
end
