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

        # Sparkplug field numbers for Payload message
        enum PayloadField
          Timestamp = 1
          Metrics   = 2
          Seq       = 3
          BdSeq     = 4
          Uuid      = 5
          Body      = 6
        end

        # Sparkplug field numbers for Metric message
        enum MetricField
          Name     = 1
          Alias    = 2
          DataType = 3
          # Value fields: 4-16 (various types)
        end

        # Maximum varint bytes (10 bytes for 64-bit)
        MAX_VARINT_BYTES = 10

        # Validates a Sparkplug payload for required fields based on message type
        # Returns ValidationResult indicating success or failure with error message
        def self.validate_payload(payload : Bytes, msg_type : MessageType) : ValidationResult
          return ValidationResult.ok if payload.empty? && (msg_type.ndeath? || msg_type.ddeath?)

          fields = parse_payload_fields(payload)
          return fields if fields.is_a?(ValidationResult) # Error result

          validate_required_fields(fields, msg_type)
        end

        # Parse payload fields and return field presence info or error
        private def self.parse_payload_fields(payload : Bytes)
          io = ::IO::Memory.new(payload)

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
                result = validate_metric(metric_bytes)
                return result unless result.valid?
              when PayloadField::Seq.value
                has_seq = true
                skip_field(io, wire_type)
              when PayloadField::BdSeq.value
                has_bdseq = true
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
          when .nbirth?, .dbirth?
            return ValidationResult.error("Missing required field 'timestamp'") unless fields[:has_timestamp]
            return ValidationResult.error("Missing required field 'metrics'") unless fields[:has_metrics]
            return ValidationResult.error("BIRTH message must contain at least one metric") if fields[:metric_count] == 0
            return ValidationResult.error("Missing required field 'seq'") unless fields[:has_seq]
            return ValidationResult.error("Missing required field 'bdSeq'") unless fields[:has_bdseq]
          when .ndata?, .ddata?
            return ValidationResult.error("Missing required field 'seq'") unless fields[:has_seq]
          when .ndeath?, .ddeath?
            return ValidationResult.error("Missing required field 'bdSeq'") unless fields[:has_bdseq]
          end

          ValidationResult.ok
        end

        # Validate a single Metric message structure
        private def self.validate_metric(bytes : Bytes) : ValidationResult
          io = ::IO::Memory.new(bytes)
          io.rewind

          has_name = false
          has_datatype = false

          begin
            while io.pos < io.size
              tag = read_varint(io)
              field_number = (tag >> 3).to_u32
              wire_type = WireType.new((tag & 0x7).to_i32)

              case field_number
              when MetricField::Name.value
                has_name = true
                skip_field(io, wire_type)
              when MetricField::DataType.value
                has_datatype = true
                skip_field(io, wire_type)
              else
                # Other fields (alias, value fields, etc.)
                skip_field(io, wire_type)
              end
            end
          rescue ex : ::IO::Error
            return ValidationResult.error("Invalid metric structure: #{ex.message}")
          rescue ex : ArgumentError
            return ValidationResult.error("Invalid metric structure: #{ex.message}")
          end

          return ValidationResult.error("Metric missing required field 'name'") unless has_name
          return ValidationResult.error("Metric missing required field 'datatype'") unless has_datatype

          ValidationResult.ok
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
