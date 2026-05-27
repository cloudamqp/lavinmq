module LavinMQ
  module MQTT
    module Sparkplug
      module CertificateMapper
        CERTIFICATE_PREFIX = "$sparkplug/certificates/"

        # Expands a $sparkplug/certificates/# wildcard into actual BIRTH topic patterns
        # and yields each expanded topic filter to subscribe to.
        #
        # Examples:
        #   "$sparkplug/certificates/#" -> "spBv3.0/+/NBIRTH/+", "spBv3.0/+/DBIRTH/+/+"
        #   "$sparkplug/certificates/group1/#" -> "spBv3.0/group1/NBIRTH/+", "spBv3.0/group1/DBIRTH/+/+"
        #   "$sparkplug/certificates/group1/NBIRTH/#" -> "spBv3.0/group1/NBIRTH/+"
        #   "$sparkplug/certificates/group1/NBIRTH/node1" -> "spBv3.0/group1/NBIRTH/node1"
        #   "$sparkplug/certificates/group1/DBIRTH/#" -> "spBv3.0/group1/DBIRTH/+/+"
        #   "$sparkplug/certificates/group1/DBIRTH/node1/#" -> "spBv3.0/group1/DBIRTH/node1/+"
        # ameba:disable Metrics/CyclomaticComplexity
        def self.expand_certificate_subscription(filter : String, & : String ->)
          # Pass through anything that isn't a certificate topic
          unless filter.starts_with?(CERTIFICATE_PREFIX)
            yield filter
            return
          end

          suffix = filter[CERTIFICATE_PREFIX.size..]
          # A certificate topic has at most 4 segments after the prefix
          segments = Array(String).new(4)
          suffix.split('/') { |segment| segments << segment }

          case segments.size
          when 1
            if segments[0] == "#"
              # "$sparkplug/certificates/#" - all BIRTH certificates
              yield "spBv3.0/+/NBIRTH/+"
              yield "spBv3.0/+/DBIRTH/+/+"
            end
            # else "$sparkplug/certificates/group1" - invalid, need more segments
          when 2
            group_id = segments[0]
            if segments[1] == "#"
              # "$sparkplug/certificates/group1/#" - all BIRTH for group
              yield "spBv3.0/#{group_id}/NBIRTH/+"
              yield "spBv3.0/#{group_id}/DBIRTH/+/+"
            end
            # else "$sparkplug/certificates/group1/NBIRTH" - invalid, need more segments
          when 3
            group_id = segments[0]
            msg_type = segments[1]
            third = segments[2]

            if msg_type == "NBIRTH"
              if third == "#"
                # "$sparkplug/certificates/group1/NBIRTH/#" - all NBIRTH for group
                yield "spBv3.0/#{group_id}/NBIRTH/+"
              else
                # "$sparkplug/certificates/group1/NBIRTH/node1" - specific NBIRTH
                yield "spBv3.0/#{group_id}/NBIRTH/#{third}"
              end
            elsif msg_type == "DBIRTH" && third == "#"
              # "$sparkplug/certificates/group1/DBIRTH/#" - all DBIRTH for group
              yield "spBv3.0/#{group_id}/DBIRTH/+/+"
            end
            # else invalid (DBIRTH needs device_id, or unknown message type)
          when 4
            group_id = segments[0]
            msg_type = segments[1]
            edge_node_id = segments[2]
            fourth = segments[3]

            if msg_type == "DBIRTH"
              if fourth == "#"
                # "$sparkplug/certificates/group1/DBIRTH/node1/#" - all DBIRTH for edge node
                yield "spBv3.0/#{group_id}/DBIRTH/#{edge_node_id}/+"
              else
                # "$sparkplug/certificates/group1/DBIRTH/node1/device1" - specific DBIRTH
                yield "spBv3.0/#{group_id}/DBIRTH/#{edge_node_id}/#{fourth}"
              end
            end
            # else NBIRTH doesn't have device level, this is invalid
          end
        end

        # Check if topic is a certificate access topic
        def self.certificate_topic?(topic : String) : Bool
          topic.starts_with?(CERTIFICATE_PREFIX)
        end
      end
    end
  end
end
