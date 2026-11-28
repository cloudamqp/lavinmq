require "json"
require "./connection"

module LavinMQ
  module Shovel
    module Postgres
      # Decodes the pgoutput logical replication protocol (version 1) and
      # collects each transaction's row changes into one JSON document:
      #
      #   {"xid":123,"lsn":"0/16B3748","commit_time":"2026-10-15T12:00:00.123456Z",
      #    "changes":[{"op":"insert","schema":"public","table":"t","new":{...}}, ...]}
      #
      # Feed it the payload of every XLogData message with #decode; it returns
      # a Transaction when a Commit completes one.
      class PgOutput
        record Column, name : String, type_oid : UInt32, key : Bool
        record Relation, schema : String, table : String, columns : Array(Column)

        record Transaction,
          xid : UInt32,
          commit_lsn : UInt64,
          end_lsn : UInt64,
          commit_time : Time,
          change_count : Int32,
          body : Bytes do
          def empty? : Bool
            change_count.zero?
          end
        end

        # Type OIDs whose text representation is emitted as JSON as-is
        BOOL_OID    =   16_u32
        INT8_OID    =   20_u32
        INT2_OID    =   21_u32
        INT4_OID    =   23_u32
        OID_OID     =   26_u32
        JSON_OID    =  114_u32
        FLOAT4_OID  =  700_u32
        FLOAT8_OID  =  701_u32
        NUMERIC_OID = 1700_u32
        JSONB_OID   = 3802_u32

        @relations = Hash(UInt32, Relation).new
        @changes = IO::Memory.new
        @change_count = 0
        @xid = 0_u32
        getter? in_transaction = false

        def decode(data : Bytes) : Transaction?
          io = IO::Memory.new(data, writable: false)
          type = io.read_byte || raise Error.new("Empty pgoutput message")
          case type.unsafe_chr
          when 'B' then begin_tx(io)
          when 'C' then return commit(io)
          when 'R' then relation(io)
          when 'I' then insert(io)
          when 'U' then update(io)
          when 'D' then delete(io)
          when 'T' then truncate(io)
          when 'Y', 'O', 'M'
            # Type, Origin and logical decoding messages carry no row changes
          else
            raise Error.new("Unknown pgoutput message type '#{type.unsafe_chr}'")
          end
          nil
        end

        private def begin_tx(io)
          read_u64(io) # final LSN, repeated in Commit
          read_i64(io) # commit time, repeated in Commit
          @xid = read_u32(io)
          @changes.clear
          @change_count = 0
          @in_transaction = true
        end

        private def commit(io) : Transaction
          io.read_byte # flags, unused
          commit_lsn = read_u64(io)
          end_lsn = read_u64(io)
          commit_time = Postgres.to_time(read_i64(io))
          @in_transaction = false
          # Written directly rather than through JSON::Builder#raw, which takes
          # a String: that would copy a large transaction's changes twice more.
          # The LSN and the timestamp need no escaping.
          body = IO::Memory.new(@changes.bytesize + 128)
          body << %({"xid":) << @xid << %(,"lsn":") << LSN.format(commit_lsn)
          body << %(","commit_time":") << commit_time.to_rfc3339(fraction_digits: 6) << %(","changes":[)
          body.write @changes.to_slice
          body << "]}"
          Transaction.new(@xid, commit_lsn, end_lsn, commit_time, @change_count, body.to_slice)
        end

        private def relation(io)
          id = read_u32(io)
          schema = read_cstring(io)
          table = read_cstring(io)
          io.read_byte # replica identity setting
          count = read_i16(io)
          columns = Array(Column).new(count) do
            flags = io.read_byte || raise Error.new("Truncated Relation message")
            name = read_cstring(io)
            type_oid = read_u32(io)
            read_i32(io) # type modifier
            Column.new(name, type_oid, flags & 1 == 1)
          end
          @relations[id] = Relation.new(schema, table, columns)
        end

        private def insert(io)
          rel = relation_for(read_u32(io))
          expect(io, 'N')
          change("insert", rel) do |json|
            json.field("new") { tuple(io, rel, json) }
          end
        end

        private def update(io)
          rel = relation_for(read_u32(io))
          change("update", rel) do |json|
            kind = expect(io, 'K', 'O', 'N')
            unless kind == 'N'
              old_tuple(io, rel, kind, json)
              expect(io, 'N')
            end
            json.field("new") { tuple(io, rel, json) }
          end
        end

        private def delete(io)
          rel = relation_for(read_u32(io))
          change("delete", rel) do |json|
            old_tuple(io, rel, expect(io, 'K', 'O'), json)
          end
        end

        private def truncate(io)
          count = read_i32(io)
          io.read_byte # options (cascade, restart identity)
          count.times do
            rel = relation_for(read_u32(io))
            change("truncate", rel) { }
          end
        end

        # A 'K' tuple holds the replica identity (key) columns, the rest are
        # sent as nulls and left out; an 'O' tuple (REPLICA IDENTITY FULL) is
        # the whole old row.
        private def old_tuple(io, rel, kind : Char, json)
          if kind == 'K'
            json.field("key") { tuple(io, rel, json, key_only: true) }
          else
            json.field("old") { tuple(io, rel, json) }
          end
        end

        private def change(op : String, rel : Relation, &)
          @changes << ',' unless @change_count.zero?
          JSON.build(@changes) do |json|
            json.object do
              json.field "op", op
              json.field "schema", rel.schema
              json.field "table", rel.table
              yield json
            end
          end
          @change_count += 1
        end

        private def tuple(io, rel : Relation, json : JSON::Builder, key_only = false)
          count = read_i16(io)
          json.object do
            count.times do |i|
              col = rel.columns[i]? || raise Error.new("Tuple has more columns than relation #{rel.table}")
              kind = io.read_byte || raise Error.new("Truncated tuple")
              case kind.unsafe_chr
              when 'n'
                json.field(col.name, nil) if col.key || !key_only
              when 'u'
                # Unchanged TOASTed value, not sent by the server
              when 't'
                len = read_i32(io)
                value = io.read_string(len)
                next if key_only && !col.key
                json.field(col.name) { value(json, col.type_oid, value) }
              when 'b'
                len = read_i32(io)
                io.skip(len)
                raise Error.new("Binary tuple values are not supported")
              else
                raise Error.new("Unknown tuple value kind '#{kind.unsafe_chr}'")
              end
            end
          end
        end

        private def value(json : JSON::Builder, type_oid : UInt32, value : String)
          case type_oid
          when BOOL_OID
            json.bool(value == "t")
          when INT2_OID, INT4_OID, INT8_OID, OID_OID
            json.raw(value)
          when FLOAT4_OID, FLOAT8_OID, NUMERIC_OID
            # NaN and ±Infinity have no JSON number representation
            if value[0]?.try { |c| c.ascii_number? || c == '-' } && !value.ends_with?("Infinity")
              json.raw(value)
            else
              json.string(value)
            end
          when JSON_OID, JSONB_OID
            json.raw(value)
          else
            json.string(value)
          end
        end

        private def relation_for(id : UInt32) : Relation
          @relations[id]? || raise Error.new("Unknown relation #{id}")
        end

        private def expect(io, *kinds : Char) : Char
          b = io.read_byte || raise Error.new("Truncated pgoutput message")
          c = b.unsafe_chr
          raise Error.new("Unexpected tuple type '#{c}'") unless kinds.includes?(c)
          c
        end

        private def read_cstring(io) : String
          io.gets('\0', chomp: true) || raise Error.new("Truncated pgoutput message")
        end

        private def read_u64(io) : UInt64
          io.read_bytes(UInt64, IO::ByteFormat::BigEndian)
        end

        private def read_i64(io) : Int64
          io.read_bytes(Int64, IO::ByteFormat::BigEndian)
        end

        private def read_u32(io) : UInt32
          io.read_bytes(UInt32, IO::ByteFormat::BigEndian)
        end

        private def read_i32(io) : Int32
          io.read_bytes(Int32, IO::ByteFormat::BigEndian)
        end

        private def read_i16(io) : Int16
          io.read_bytes(Int16, IO::ByteFormat::BigEndian)
        end
      end
    end
  end
end
