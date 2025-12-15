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
        property namespace : String
        property group_id : String
        property message_type : MessageType
        property edge_node_id : String
        property device_id : String?

        def initialize(@namespace, @group_id, @message_type, @edge_node_id, @device_id = nil)
        end
      end

      module Validator
        SPARKPLUG_NAMESPACE = "spBv3.0"
        CERTIFICATE_PREFIX  = "$sparkplug/certificates/"

        # Validates Sparkplug 3.0 topic format
        # Returns: MessageType if valid Sparkplug topic
        # Returns: nil if certificate topic (valid but not a message type)
        # Raises: ValidationError if invalid Sparkplug topic
        def self.validate_topic(topic : String) : MessageType?
          # Certificate topics are valid for subscriptions only
          return nil if topic.starts_with?(CERTIFICATE_PREFIX)

          # Parse Sparkplug topic
          parts = parse_topic(topic)
          return nil unless parts

          # Validate namespace
          unless parts.namespace == SPARKPLUG_NAMESPACE
            raise ValidationError.new(
              "Invalid namespace '#{parts.namespace}', expected '#{SPARKPLUG_NAMESPACE}' in topic: #{topic}"
            )
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

          # Validate device_id if present
          if device_id = parts.device_id
            validate_identifier(device_id, "device_id", topic)
          end

          # Check if device_id is required for this message type
          if parts.message_type.requires_device_id? && parts.device_id.nil?
            raise ValidationError.new(
              "Message type #{parts.message_type} requires device_id in topic: #{topic}"
            )
          end

          parts.message_type
        end

        # Parse Sparkplug topic into components
        # Returns: TopicParts if valid format, nil if not a Sparkplug topic
        def self.parse_topic(topic : String) : TopicParts?
          # Expected format: spBv3.0/{group_id}/{message_type}/{edge_node_id}/{device_id?}
          segments = topic.split('/')
          return nil if segments.size < 4

          namespace = segments[0]
          group_id = segments[1]
          message_type_str = segments[2]
          edge_node_id = segments[3]
          device_id = segments.size > 4 ? segments[4] : nil

          # Parse message type
          message_type = parse_message_type(message_type_str)
          unless message_type
            raise ValidationError.new(
              "Unknown message type '#{message_type_str}' in topic: #{topic}"
            )
          end

          TopicParts.new(namespace, group_id, message_type, edge_node_id, device_id)
        rescue ArgumentError
          nil
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

        # Validate identifier contains only allowed characters
        # Sparkplug allows: alphanumeric, dash, underscore, period
        private def self.validate_identifier(id : String, field_name : String, topic : String)
          unless id.matches?(/^[a-zA-Z0-9_.\-]+$/)
            raise ValidationError.new(
              "Invalid #{field_name} '#{id}' contains disallowed characters in topic: #{topic}"
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
