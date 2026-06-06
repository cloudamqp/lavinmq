module LavinMQ
  module MQTT
    module Sparkplug
      class ValidationError < Exception
        getter reason_code : UInt8

        def initialize(message : String, @reason_code : UInt8 = 0x87_u8)
          super(message)
        end
      end

      enum MessageType
        NBIRTH
        NDEATH
        DBIRTH
        DDEATH
        NDATA
        DDATA
        NCMD
        DCMD
        STATE

        def requires_device_id?
          self.dbirth? || self.ddeath? || self.ddata? || self.dcmd?
        end
      end

      struct TopicParts
        getter topic : String
        property namespace : String
        property group_id : String
        property message_type : MessageType
        property edge_node_id : String
        property device_id : String?

        def initialize(@topic, @namespace, @group_id, @message_type, @edge_node_id, @device_id = nil)
        end
      end

      module Validator
        # The Sparkplug B MQTT topic namespace element. Note that even in the
        # Sparkplug 3.0.0 specification this constant remains "spBv1.0" — the
        # "3.0.0" is the specification document version, not the namespace.
        SPARKPLUG_NAMESPACE = "spBv1.0"
        # Precomputed "<namespace>/" prefix so hot-path topic checks don't allocate.
        NAMESPACE_PREFIX   = "#{SPARKPLUG_NAMESPACE}/"
        CERTIFICATE_PREFIX = "$sparkplug/certificates/"

        # Validates the parsed parts of a Sparkplug 3.0 topic
        # Returns: the topic's MessageType if valid
        # Raises: ValidationError if invalid
        def self.validate_topic(parts : TopicParts) : MessageType
          topic = parts.topic

          # Validate namespace
          unless parts.namespace == SPARKPLUG_NAMESPACE
            raise ValidationError.new(
              "Invalid namespace '#{parts.namespace}', expected '#{SPARKPLUG_NAMESPACE}' in topic: #{topic}"
            )
          end

          # Host STATE certificate (spBv1.0/STATE/{host_id}) has no group_id;
          # the host_id is carried in edge_node_id. Validate just that and return.
          if parts.message_type.state? && parts.group_id.empty?
            if parts.edge_node_id.empty?
              raise ValidationError.new("Missing host_id in topic: #{topic}")
            end
            validate_identifier(parts.edge_node_id, "host_id", topic)
            return parts.message_type
          end

          # Validate group_id
          if parts.group_id.empty?
            raise ValidationError.new("Missing group_id in topic: #{topic}")
          end
          validate_identifier(parts.group_id, "group_id", topic)

          # Validate edge_node_id
          if parts.edge_node_id.empty?
            raise ValidationError.new("Missing edge_node_id in topic: #{topic}")
          end
          validate_identifier(parts.edge_node_id, "edge_node_id", topic)

          # Validate device_id if present (its presence/absence per message type
          # is already enforced by the topic-level count check in #parse_topic)
          if device_id = parts.device_id
            validate_identifier(device_id, "device_id", topic)
          end

          parts.message_type
        end

        # Parse Sparkplug topic into components
        # Returns: TopicParts if valid format, nil if not a Sparkplug topic
        # Raises: ValidationError if the message type is unknown
        def self.parse_topic(topic : String) : TopicParts?
          # Expected format: spBv1.0/{group_id}/{message_type}/{edge_node_id}/{device_id?}
          # A Sparkplug topic has at most 5 segments
          segments = Array(String).new(5)
          topic.split('/') { |segment| segments << segment }

          # Host STATE certificate: spBv1.0/STATE/{host_id} (no group_id/edge_node_id)
          if segments.size == 3 && segments[1] == "STATE"
            return TopicParts.new(topic, segments[0], "", MessageType::STATE, segments[2])
          end

          return nil if segments.size < 4

          namespace = segments[0]
          group_id = segments[1]
          message_type_str = segments[2]
          edge_node_id = segments[3]

          # Parse message type
          message_type = parse_message_type(message_type_str)
          unless message_type
            raise ValidationError.new(
              "Unknown message type '#{message_type_str}' in topic: #{topic}"
            )
          end

          # Enforce the exact topic-level count for the message type:
          #   device-level (DBIRTH/DDEATH/DDATA/DCMD) -> 5 segments (device_id required)
          #   node-level   (everything else)          -> 4 segments (no device_id)
          if message_type.requires_device_id?
            if segments.size < 5
              raise ValidationError.new("Message type #{message_type} requires device_id in topic: #{topic}")
            elsif segments.size > 5
              raise ValidationError.new("Message type #{message_type} has too many topic levels (#{segments.size}) in topic: #{topic}")
            end
          elsif segments.size > 4
            raise ValidationError.new("Message type #{message_type} must not have a device_id in topic: #{topic}")
          end

          device_id = segments.size > 4 ? segments[4] : nil
          TopicParts.new(topic, namespace, group_id, message_type, edge_node_id, device_id)
        end

        # Parse message type string to enum
        private def self.parse_message_type(str : String) : MessageType?
          case str
          when "NBIRTH" then MessageType::NBIRTH
          when "NDEATH" then MessageType::NDEATH
          when "DBIRTH" then MessageType::DBIRTH
          when "DDEATH" then MessageType::DDEATH
          when "NDATA"  then MessageType::NDATA
          when "DDATA"  then MessageType::DDATA
          when "NCMD"   then MessageType::NCMD
          when "DCMD"   then MessageType::DCMD
          when "STATE"  then MessageType::STATE
          else               nil
          end
        end

        # Validate identifier characters. Per the Sparkplug spec, Group ID, Edge
        # Node ID and Device ID MUST be valid UTF-8 strings with the exception of
        # the reserved MQTT characters '+', '/' and '#'. (The '/' can never appear
        # here since identifiers are produced by splitting the topic on '/'.)
        # Anything else — including spaces and non-ASCII — is allowed.
        private def self.validate_identifier(id : String, field_name : String, topic : String)
          if id.includes?('+') || id.includes?('#')
            raise ValidationError.new(
              "Invalid #{field_name} '#{id}' contains a reserved character in topic: #{topic}"
            )
          end
        end

        # Check if topic is a certificate access topic
        def self.certificate_topic?(topic : String) : Bool
          topic.starts_with?(CERTIFICATE_PREFIX)
        end

        # Check if topic is any Sparkplug-related topic
        def self.sparkplug_topic?(topic : String) : Bool
          topic.starts_with?(SPARKPLUG_NAMESPACE) || topic.starts_with?(CERTIFICATE_PREFIX)
        end
      end
    end
  end
end
