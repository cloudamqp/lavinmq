require "json"
require "base64"
require "amq-protocol"

module LavinMQ
  # Type-aware JSON (de)serialization of AMQP argument tables for the
  # definitions snapshots and WAL.
  #
  # JSON represents most AMQP field types natively (bool, number, string,
  # array, table). The two it can't express are byte arrays and timestamps —
  # plain `Table#to_json` degrades those to a (base64) string and an ISO-8601
  # string, so a restart would read them back as `String` and silently change
  # the argument's type and value. A top-level `Bytes`/`Time` argument value is
  # therefore written as a tagged object so it round-trips losslessly:
  #
  #   {"@type": "bytes",     "value": "<base64>"}
  #   {"@type": "timestamp", "value": <unix-seconds>}
  #
  # All other types stay native JSON. Integer widths collapse to Int64 and
  # Float32 to Float64, which is harmless: AMQP `Table#==` and the argument
  # readers compare numbers across widths.
  #
  # Caveat: only top-level argument values are tagged. A `Bytes`/`Time` value
  # nested inside a table- or array-valued argument still degrades to a string
  # (the library encoding) — those are vanishingly rare, and the recursive
  # `Field` alias makes a fully recursive reconstruction impractical in Crystal.
  #
  # This tagging is internal to persistence only — it must never reach the
  # untagged `/api/definitions` import reader, which would mistake a tagged
  # object for a nested table. Reading tolerates the older, untagged on-disk
  # format an upgrade may leave behind: an untagged base64/ISO string just
  # stays a string (no worse than before, no crash). A real (non-tagged) table
  # that happened to have exactly the keys `@type` (= "bytes"/"timestamp") and
  # `value` would be misread, but AMQP argument tables don't use those keys.
  module DefinitionsArguments
    extend self

    alias Field = AMQ::Protocol::Field
    alias Table = AMQ::Protocol::Table

    def to_json(json : JSON::Builder, table : Table) : Nil
      json.object do
        table.each do |key, value|
          json.field(key) { write_value(json, value) }
        end
      end
    end

    def from_json(value : JSON::Any) : Table
      hash = value.as_h?
      return Table.new unless hash
      fields = Hash(String, Field).new(hash.size)
      hash.each { |key, val| fields[key] = decode_value(val) }
      Table.new(fields)
    end

    private def write_value(json : JSON::Builder, value : Field) : Nil
      case value
      when Bytes
        json.object do
          json.field "@type", "bytes"
          json.field "value", Base64.strict_encode(value)
        end
      when Time
        json.object do
          json.field "@type", "timestamp"
          json.field "value", value.to_unix
        end
      else
        # Native JSON for scalars; the library's own encoding for nested
        # tables/arrays (lossy for Bytes/Time nested inside them, see caveat).
        value.to_json(json)
      end
    end

    private def decode_value(value : JSON::Any) : Field
      if (hash = value.as_h?) && (type = tagged_type?(hash))
        decode_tagged(type, hash["value"])
      else
        value # JSON::Any is itself a Field; the library handles native types
      end
    end

    private def tagged_type?(hash : Hash(String, JSON::Any)) : String?
      return unless hash.size == 2 && hash.has_key?("value")
      type = hash["@type"]?.try &.as_s?
      type if type.in?("bytes", "timestamp")
    end

    private def decode_tagged(type : String, value : JSON::Any) : Field
      case type
      when "bytes"     then Base64.decode(value.as_s)
      when "timestamp" then Time.unix(value.as_i64)
      else                  raise "Unknown tagged argument type #{type}"
      end
    end
  end
end
