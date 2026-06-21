require "./codec"
require "./frame"

module LavinMQ
  module AMQP10
    # Descriptor codes for performatives, message sections, delivery states and
    # SASL frames (amqp-core-transport/messaging/security-v1.0).
    module Descriptor
      OPEN        = 0x10_u64
      BEGIN       = 0x11_u64
      ATTACH      = 0x12_u64
      FLOW        = 0x13_u64
      TRANSFER    = 0x14_u64
      DISPOSITION = 0x15_u64
      DETACH      = 0x16_u64
      END_        = 0x17_u64
      CLOSE       = 0x18_u64
      ERROR       = 0x1d_u64

      RECEIVED = 0x23_u64
      ACCEPTED = 0x24_u64
      REJECTED = 0x25_u64
      RELEASED = 0x26_u64
      MODIFIED = 0x27_u64
      SOURCE   = 0x28_u64
      TARGET   = 0x29_u64

      HEADER                 = 0x70_u64
      DELIVERY_ANNOTATIONS   = 0x71_u64
      MESSAGE_ANNOTATIONS    = 0x72_u64
      PROPERTIES             = 0x73_u64
      APPLICATION_PROPERTIES = 0x74_u64
      DATA                   = 0x75_u64
      AMQP_SEQUENCE          = 0x76_u64
      AMQP_VALUE             = 0x77_u64
      FOOTER                 = 0x78_u64

      SASL_MECHANISMS = 0x40_u64
      SASL_INIT       = 0x41_u64
      SASL_CHALLENGE  = 0x42_u64
      SASL_RESPONSE   = 0x43_u64
      SASL_OUTCOME    = 0x44_u64
    end

    # Builds the positional field list of a composite type, encoding each field
    # with its correct width. Null/absent optional fields are placed as null and
    # trimmed when the list is finally written (see `Codec.write_described_list`).
    struct FieldList
      getter fields = [] of Bytes

      private def enc(& : IO ->) : Nil
        buf = IO::Memory.new
        yield buf
        @fields << buf.to_slice
      end

      def null : Nil
        @fields << Codec::NULL_BYTES
      end

      def bool(v : Bool?) : Nil
        v.nil? ? null : enc { |b| Codec.write(b, v) }
      end

      def ubyte(v : UInt8?) : Nil
        v.nil? ? null : enc { |b| Codec.write_ubyte(b, v) }
      end

      def ushort(v : UInt16?) : Nil
        v.nil? ? null : enc { |b| Codec.write_ushort(b, v) }
      end

      def uint(v : UInt32?) : Nil
        v.nil? ? null : enc { |b| Codec.write_uint(b, v) }
      end

      def ulong(v : UInt64?) : Nil
        v.nil? ? null : enc { |b| Codec.write_ulong(b, v) }
      end

      def string(v : String?) : Nil
        v.nil? ? null : enc { |b| Codec.write_string(b, v) }
      end

      def symbol(v : String?) : Nil
        v.nil? ? null : enc { |b| Codec.write_symbol(b, v) }
      end

      def binary(v : Bytes?) : Nil
        v.nil? ? null : enc { |b| Codec.write_binary(b, v) }
      end

      # Generic value (maps, arrays, nested described values).
      def value(v : Codec::AnyValue) : Nil
        v.nil? ? null : enc { |b| Codec.write(b, v) }
      end

      # A field that is itself a composite which knows how to write itself.
      def composite(c) : Nil
        c.nil? ? null : enc { |b| c.to_io(b) }
      end
    end

    # Reads the positional fields of a composite type, coercing each to the
    # expected Crystal type and defaulting absent trailing fields to null.
    struct FieldReader
      def initialize(@list : Array(Codec::AnyValue))
        @pos = 0
      end

      def self.from(d : Described) : FieldReader
        list = d.value.as?(Array(Codec::AnyValue)) || [] of Codec::AnyValue
        FieldReader.new(list)
      end

      def next_value : Codec::AnyValue
        if @pos < @list.size
          v = @list[@pos]
          @pos += 1
          v
        else
          @pos += 1
          nil
        end
      end

      def bool(default : Bool = false) : Bool
        v = next_value
        v.as?(Bool).nil? ? default : v.as(Bool)
      end

      def bool? : Bool?
        next_value.as?(Bool)
      end

      def u8?(default : UInt8? = nil) : UInt8?
        next_value.as?(UInt64).try(&.to_u8) || default
      end

      def u8(default : UInt8) : UInt8
        next_value.as?(UInt64).try(&.to_u8) || default
      end

      def u16?(default : UInt16? = nil) : UInt16?
        next_value.as?(UInt64).try(&.to_u16) || default
      end

      def u16(default : UInt16) : UInt16
        next_value.as?(UInt64).try(&.to_u16) || default
      end

      def u32?(default : UInt32? = nil) : UInt32?
        next_value.as?(UInt64).try(&.to_u32) || default
      end

      def u32(default : UInt32) : UInt32
        next_value.as?(UInt64).try(&.to_u32) || default
      end

      def u64?(default : UInt64? = nil) : UInt64?
        next_value.as?(UInt64) || default
      end

      def string? : String?
        next_value.as?(String)
      end

      def symbol? : String?
        case v = next_value
        when Symbol then v.value
        when String then v
        else             nil
        end
      end

      def bytes? : Bytes?
        next_value.as?(Bytes)
      end

      def map? : Hash(Codec::AnyValue, Codec::AnyValue)?
        next_value.as?(Hash(Codec::AnyValue, Codec::AnyValue))
      end

      def described? : Described?
        next_value.as?(Described)
      end
    end
  end
end
