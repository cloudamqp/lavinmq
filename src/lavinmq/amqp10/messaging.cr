require "amq-protocol"
require "./performatives"
require "./frames"

module LavinMQ
  module AMQP10
    # Helpers for building and classifying delivery states / outcomes
    # (amqp-core-messaging-v1.0 §3.4).
    module DeliveryState
      enum Outcome
        Accepted
        Rejected
        Released
        Modified
        Unknown
      end

      def self.accepted : Described
        Described.new(Descriptor::ACCEPTED, [] of Codec::AnyValue)
      end

      def self.released : Described
        Described.new(Descriptor::RELEASED, [] of Codec::AnyValue)
      end

      def self.rejected(condition : String? = nil, description : String? = nil) : Described
        error = if condition
                  Described.new(Descriptor::ERROR,
                    [Symbol.new(condition).as(Codec::AnyValue), description.as(Codec::AnyValue), nil] of Codec::AnyValue)
                end
        Described.new(Descriptor::REJECTED, [error.as(Codec::AnyValue)] of Codec::AnyValue)
      end

      def self.classify(state : Described?) : Outcome
        return Outcome::Unknown unless state
        case state.descriptor.as?(UInt64)
        when Descriptor::ACCEPTED then Outcome::Accepted
        when Descriptor::REJECTED then Outcome::Rejected
        when Descriptor::RELEASED then Outcome::Released
        when Descriptor::MODIFIED then Outcome::Modified
        else                           Outcome::Unknown
        end
      end
    end

    # Parses an AMQP 1.0 bare/annotated message (the `transfer` payload) into the
    # body bytes plus a best-effort mapping to AMQP 0-9-1 properties, and encodes
    # the reverse for delivery. This is the bridge that lets AMQP 1.0 and 0-9-1
    # clients share a queue (the same approach RabbitMQ takes).
    module MessageCodec
      record Parsed, properties : AMQ::Protocol::Properties, body : Bytes

      # Decode the message sections from a transfer payload.
      # ameba:disable Metrics/CyclomaticComplexity
      def self.parse(payload : Bytes) : Parsed
        io = IO::Memory.new(payload)
        props = AMQ::Protocol::Properties.new
        headers = nil
        body = Bytes.empty
        until io.pos >= payload.size
          section = Codec.read(io).as(Described)
          case section.descriptor.as?(UInt64)
          when Descriptor::HEADER
            f = FieldReader.from(section)
            durable = f.bool(false)
            props.delivery_mode = 2_u8 if durable
            if prio = f.u8?
              props.priority = prio
            end
          when Descriptor::PROPERTIES
            props = apply_properties(section, props)
          when Descriptor::APPLICATION_PROPERTIES
            if map = section.value.as?(Hash(Codec::AnyValue, Codec::AnyValue))
              headers = to_amqp_headers(map)
            end
          when Descriptor::DATA
            body = section.value.as?(Bytes) || Bytes.empty
          when Descriptor::AMQP_VALUE
            case v = section.value
            when String then body = v.to_slice.dup
            when Bytes  then body = v
            end
          else
            # delivery-annotations, message-annotations, footer, sequence: ignored for MVP
          end
        end
        props.headers = headers if headers
        Parsed.new(props, body)
      end

      # `Properties` is a struct (value type), so the mutated copy is returned.
      private def self.apply_properties(section : Described, props : AMQ::Protocol::Properties) : AMQ::Protocol::Properties
        f = FieldReader.from(section)
        if mid = f.next_value
          props.message_id = stringify(mid)
        end
        f.bytes?  # user-id
        f.string? # to
        f.string? # subject
        if reply_to = f.string?
          props.reply_to = reply_to
        end
        if cid = f.next_value
          props.correlation_id = stringify(cid)
        end
        if ct = f.symbol?
          props.content_type = ct
        end
        if ce = f.symbol?
          props.content_encoding = ce
        end
        props
      end

      private def self.stringify(v : Codec::AnyValue) : String?
        case v
        when String then v
        when Nil    then nil
        else             v.to_s
        end
      end

      private def self.to_amqp_headers(map : Hash(Codec::AnyValue, Codec::AnyValue)) : AMQ::Protocol::Table
        h = AMQ::Protocol::Table.new
        map.each do |k, v|
          key = k.is_a?(Symbol) ? k.value : k.to_s
          case v
          when String, Bool, Nil then h[key] = v
          when Int64             then h[key] = v
          when UInt64            then h[key] = v.to_i64
          when Float64           then h[key] = v
          else                        h[key] = v.to_s
          end
        end
        h
      end

      # Encode a body + AMQP 0-9-1 properties into AMQP 1.0 message sections.
      def self.encode(props : AMQ::Protocol::Properties, body : Bytes) : Bytes
        io = IO::Memory.new
        write_properties(io, props)
        if headers = props.headers
          write_application_properties(io, headers)
        end
        # data section
        Codec.write(io, Described.new(Descriptor::DATA, body.as(Codec::AnyValue)))
        io.to_slice
      end

      private def self.write_properties(io : IO, props : AMQ::Protocol::Properties) : Nil
        return unless props.message_id || props.correlation_id || props.content_type ||
                      props.content_encoding || props.reply_to
        fl = FieldList.new
        fl.string props.message_id
        fl.binary nil # user-id
        fl.string nil # to
        fl.string nil # subject
        fl.string props.reply_to
        fl.string props.correlation_id
        fl.symbol props.content_type
        fl.symbol props.content_encoding
        Codec.write_described_list(io, Descriptor::PROPERTIES, fl.fields)
      end

      private def self.write_application_properties(io : IO, headers : AMQ::Protocol::Table) : Nil
        map = Hash(Codec::AnyValue, Codec::AnyValue).new
        headers.each do |k, v|
          map[k] = case v
                   when String, Bool, Nil then v
                   when Int               then v.to_i64
                   when Float             then v.to_f64
                   else                        v.to_s
                   end.as(Codec::AnyValue)
        end
        Codec.write(io, Described.new(Descriptor::APPLICATION_PROPERTIES, map.as(Codec::AnyValue)))
      end
    end
  end
end
