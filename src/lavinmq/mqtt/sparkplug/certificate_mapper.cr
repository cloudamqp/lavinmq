module LavinMQ
  module MQTT
    module Sparkplug
      module CertificateMapper
        CERTIFICATE_PREFIX = "$sparkplug/certificates/"

        # Expands $sparkplug/certificates/# wildcard into actual BIRTH topic patterns
        # Returns array of actual topic filters to subscribe to
        #
        # Examples:
        #   "$sparkplug/certificates/#" -> ["spBv3.0/+/NBIRTH/+", "spBv3.0/+/DBIRTH/+/+"]
        #   "$sparkplug/certificates/group1/#" -> ["spBv3.0/group1/NBIRTH/+", "spBv3.0/group1/DBIRTH/+/+"]
        #   "$sparkplug/certificates/group1/NBIRTH/#" -> ["spBv3.0/group1/NBIRTH/+"]
        #   "$sparkplug/certificates/group1/NBIRTH/node1" -> ["spBv3.0/group1/NBIRTH/node1"]
        #   "$sparkplug/certificates/group1/DBIRTH/#" -> ["spBv3.0/group1/DBIRTH/+/+"]
        #   "$sparkplug/certificates/group1/DBIRTH/node1/#" -> ["spBv3.0/group1/DBIRTH/node1/+"]
        # ameba:disable Metrics/CyclomaticComplexity
        def self.expand_certificate_subscription(filter : String) : Array(String)
          # Remove certificate prefix
          return [filter] unless filter.starts_with?(CERTIFICATE_PREFIX)

          suffix = filter[CERTIFICATE_PREFIX.size..]
          segments = suffix.split('/')

          case segments.size
          when 0
            # "$sparkplug/certificates/" - invalid, return empty
            [] of String
          when 1
            if segments[0] == "#"
              # "$sparkplug/certificates/#" - all BIRTH certificates
              [
                "spBv3.0/+/NBIRTH/+",
                "spBv3.0/+/DBIRTH/+/+",
              ]
            else
              # "$sparkplug/certificates/group1" - invalid, need more segments
              [] of String
            end
          when 2
            group_id = segments[0]
            second = segments[1]

            if second == "#"
              # "$sparkplug/certificates/group1/#" - all BIRTH for group
              [
                "spBv3.0/#{group_id}/NBIRTH/+",
                "spBv3.0/#{group_id}/DBIRTH/+/+",
              ]
            else
              # "$sparkplug/certificates/group1/NBIRTH" - invalid, need more segments
              [] of String
            end
          when 3
            group_id = segments[0]
            msg_type = segments[1]
            third = segments[2]

            if msg_type == "NBIRTH"
              if third == "#"
                # "$sparkplug/certificates/group1/NBIRTH/#" - all NBIRTH for group
                ["spBv3.0/#{group_id}/NBIRTH/+"]
              else
                # "$sparkplug/certificates/group1/NBIRTH/node1" - specific NBIRTH
                ["spBv3.0/#{group_id}/NBIRTH/#{third}"]
              end
            elsif msg_type == "DBIRTH"
              if third == "#"
                # "$sparkplug/certificates/group1/DBIRTH/#" - all DBIRTH for group
                ["spBv3.0/#{group_id}/DBIRTH/+/+"]
              else
                # "$sparkplug/certificates/group1/DBIRTH/node1" - invalid, need device_id
                [] of String
              end
            else
              # Invalid message type for certificates (only NBIRTH/DBIRTH are certificates)
              [] of String
            end
          when 4
            group_id = segments[0]
            msg_type = segments[1]
            edge_node_id = segments[2]
            fourth = segments[3]

            if msg_type == "DBIRTH"
              if fourth == "#"
                # "$sparkplug/certificates/group1/DBIRTH/node1/#" - all DBIRTH for edge node
                ["spBv3.0/#{group_id}/DBIRTH/#{edge_node_id}/+"]
              else
                # "$sparkplug/certificates/group1/DBIRTH/node1/device1" - specific DBIRTH
                ["spBv3.0/#{group_id}/DBIRTH/#{edge_node_id}/#{fourth}"]
              end
            else
              # NBIRTH doesn't have device level, this is invalid
              [] of String
            end
          else
            # Too many segments, invalid
            [] of String
          end
        end

        # Check if topic is a certificate access topic
        def self.certificate_topic?(topic : String) : Bool
          topic.starts_with?(CERTIFICATE_PREFIX)
        end

        # Parse certificate topic into components
        # Returns: {group_id?, message_type?, edge_node_id?, device_id?}
        def self.parse_certificate_topic(topic : String) : Tuple(String?, String?, String?, String?)?
          return nil unless topic.starts_with?(CERTIFICATE_PREFIX)

          suffix = topic[CERTIFICATE_PREFIX.size..]
          segments = suffix.split('/')

          case segments.size
          when 0, 1
            {nil, nil, nil, nil}
          when 2
            {segments[0] == "#" ? nil : segments[0], nil, nil, nil}
          when 3
            group_id = segments[0]
            msg_type = segments[1]
            edge_node = segments[2] == "#" ? nil : segments[2]
            {group_id, msg_type, edge_node, nil}
          when 4
            group_id = segments[0]
            msg_type = segments[1]
            edge_node_id = segments[2]
            device_id = segments[3] == "#" ? nil : segments[3]
            {group_id, msg_type, edge_node_id, device_id}
          else
            nil
          end
        end
      end
    end
  end
end
