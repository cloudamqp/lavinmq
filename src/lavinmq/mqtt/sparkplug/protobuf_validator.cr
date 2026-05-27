module LavinMQ
  module MQTT
    module Sparkplug
      # Result of protobuf payload validation
      struct ValidationResult
        getter? valid : Bool
        getter error : String?

        def initialize(@valid : Bool, @error : String? = nil)
        end

        def self.ok : ValidationResult
          new(valid: true)
        end

        def self.error(message : String) : ValidationResult
          new(valid: false, error: message)
        end
      end

      module ProtobufValidator
        # Protobuf wire types
        enum WireType
          Varint          = 0
          Fixed64         = 1
          LengthDelimited = 2
          Fixed32         = 5
        end

        # Sparkplug field numbers for the top-level Payload message.
        # Note: there is no top-level bdSeq field — bdSeq is carried as a Metric
        # named "bdSeq" (see #validate_metric / has_bdseq detection).
        enum PayloadField
          Timestamp = 1
          Metrics   = 2
          Seq       = 3
          Uuid      = 4
          Body      = 5
        end

        # Sparkplug field numbers for the Metric message.
        enum MetricField
          Name      = 1
          Alias     = 2
          Timestamp = 3
          DataType  = 4
          # Value fields: 10-19 (various types)
        end

        # Metric name carrying the birth/death sequence number.
        BDSEQ_METRIC_NAME = "bdSeq"

        # Maximum varint bytes (10 bytes for 64-bit)
        MAX_VARINT_BYTES = 10

        # Validates a Sparkplug payload for required fields based on message type
        # Returns ValidationResult indicating success or failure with error message
        def self.validate_payload(payload : Bytes, msg_type : MessageType) : ValidationResult
          # STATE messages are plain text/JSON, not protobuf payloads
          return ValidationResult.ok if msg_type.state?

          return ValidationResult.ok if payload.empty? && (msg_type.ndeath? || msg_type.ddeath?)

          fields = parse_payload_fields(payload, msg_type)
          return fields if fields.is_a?(ValidationResult) # Error result

          validate_required_fields(fields, msg_type)
        end

        # Parse payload fields and return field presence info or error.
        # On BIRTH messages every metric MUST carry name + datatype; on DATA/CMD
        # messages metrics are referenced by alias and the name MUST be omitted,
        # so name/datatype are not required there.
        private def self.parse_payload_fields(payload : Bytes, msg_type : MessageType)
          io = ::IO::Memory.new(payload)
          require_metric_name_datatype = msg_type.nbirth? || msg_type.dbirth?

          has_timestamp = false
          has_metrics = false
          has_seq = false
          has_bdseq = false
          metric_count = 0

          begin
            while io.pos < io.size
              tag = read_varint(io)
              field_number = (tag >> 3).to_u32
              wire_type = WireType.new((tag & 0x7).to_i32)

              case field_number
              when PayloadField::Timestamp.value
                has_timestamp = true
                skip_field(io, wire_type)
              when PayloadField::Metrics.value
                has_metrics = true
                metric_count += 1
                unless wire_type.length_delimited?
                  return ValidationResult.error("Metrics field has incorrect wire type")
                end
                # Slice the metric out of the backing buffer (no copy) and validate it
                length = read_length(io)
                metric_bytes = payload[io.pos, length]
                io.skip(length)
                result, name = validate_metric(metric_bytes, require_metric_name_datatype)
                return result unless result.valid?
                # bdSeq is conveyed as a Metric named "bdSeq", not a top-level field.
                has_bdseq = true if name == BDSEQ_METRIC_NAME
              when PayloadField::Seq.value
                has_seq = true
                skip_field(io, wire_type)
              else
                skip_field(io, wire_type)
              end
            end
          rescue ex : ::IO::Error
            return ValidationResult.error("Invalid protobuf wire format: #{ex.message}")
          rescue ex : ArgumentError
            return ValidationResult.error("Invalid protobuf wire format: #{ex.message}")
          end

          {
            has_timestamp: has_timestamp,
            has_metrics:   has_metrics,
            has_seq:       has_seq,
            has_bdseq:     has_bdseq,
            metric_count:  metric_count,
          }
        end

        # Validate required fields based on message type
        private def self.validate_required_fields(fields, msg_type : MessageType) : ValidationResult
          case msg_type
          when .nbirth?
            return ValidationResult.error("Missing required field 'timestamp'") unless fields[:has_timestamp]
            return ValidationResult.error("Missing required field 'seq'") unless fields[:has_seq]
            return ValidationResult.error("Missing required metric 'bdSeq'") unless fields[:has_bdseq]
          when .dbirth?
            return ValidationResult.error("Missing required field 'timestamp'") unless fields[:has_timestamp]
            return ValidationResult.error("Missing required field 'metrics'") unless fields[:has_metrics]
            return ValidationResult.error("BIRTH message must contain at least one metric") if fields[:metric_count] == 0
            return ValidationResult.error("Missing required field 'seq'") unless fields[:has_seq]
          when .ndata?, .ddata?, .ddeath?
            return ValidationResult.error("Missing required field 'seq'") unless fields[:has_seq]
          when .ndeath?
            return ValidationResult.error("Missing required metric 'bdSeq'") unless fields[:has_bdseq]
          end

          ValidationResult.ok
        end

        # Validate a single Metric message structure. Returns the validation
        # result together with the metric's name (nil if absent), so the caller
        # can detect the special "bdSeq" metric. When +require_name_datatype+ is
        # true (BIRTH messages) the metric MUST carry both a name and a datatype;
        # otherwise (DATA/CMD alias flows) those are optional.
        private def self.validate_metric(bytes : Bytes, require_name_datatype : Bool) : {ValidationResult, String?}
          io = ::IO::Memory.new(bytes)

          name = nil
          has_datatype = false

          begin
            while io.pos < io.size
              tag = read_varint(io)
              field_number = (tag >> 3).to_u32
              wire_type = WireType.new((tag & 0x7).to_i32)

              case field_number
              when MetricField::Name.value
                unless wire_type.length_delimited?
                  return {ValidationResult.error("Metric name has incorrect wire type"), nil}
                end
                length = read_length(io)
                name = String.new(bytes[io.pos, length])
                io.skip(length)
              when MetricField::DataType.value
                has_datatype = true
                skip_field(io, wire_type)
              else
                # Other fields (alias, timestamp, value fields, etc.)
                skip_field(io, wire_type)
              end
            end
          rescue ex : ::IO::Error
            return {ValidationResult.error("Invalid metric structure: #{ex.message}"), nil}
          rescue ex : ArgumentError
            return {ValidationResult.error("Invalid metric structure: #{ex.message}"), nil}
          end

          if require_name_datatype
            return {ValidationResult.error("Metric missing required field 'name'"), name} if name.nil?
            return {ValidationResult.error("Metric missing required field 'datatype'"), name} unless has_datatype
          end

          {ValidationResult.ok, name}
        end

        # Read a protobuf varint from IO
        # Returns the decoded uint64 value
        private def self.read_varint(io : ::IO) : UInt64
          result = 0_u64
          shift = 0

          MAX_VARINT_BYTES.times do
            byte = io.read_byte
            raise ::IO::EOFError.new("Unexpected EOF while reading varint") if byte.nil?

            # Add the lower 7 bits to result
            result |= ((byte & 0x7F).to_u64 << shift)

            # If MSB is 0, we're done
            return result if (byte & 0x80) == 0

            shift += 7
          end

          raise ArgumentError.new("Varint exceeds maximum length")
        end

        # Read a length-delimited field's length, bounds-checked against the
        # bytes remaining in `io` so a bogus length can't over-read or overflow.
        private def self.read_length(io : ::IO::Memory) : Int32
          length = read_varint(io)
          remaining = (io.size - io.pos).to_u64
          if length > remaining
            raise ::IO::Error.new("Length #{length} exceeds #{remaining} remaining bytes")
          end
          length.to_i32
        end

        # Skip a field based on its wire type
        private def self.skip_field(io : ::IO::Memory, wire_type : WireType) : Nil
          case wire_type
          when .varint?
            # Read and discard varint
            read_varint(io)
          when .fixed64?
            # Skip 8 bytes
            io.skip(8)
          when .length_delimited?
            # Read length, then skip that many bytes
            io.skip(read_length(io))
          when .fixed32?
            # Skip 4 bytes
            io.skip(4)
          else
            raise ArgumentError.new("Unknown wire type: #{wire_type}")
          end
        end
      end
    end
  end
end
