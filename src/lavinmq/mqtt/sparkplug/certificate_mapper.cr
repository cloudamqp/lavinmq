require "./validator"

module LavinMQ
  module MQTT
    module Sparkplug
      module CertificateMapper
        CERTIFICATE_PREFIX = "$sparkplug/certificates/"

        # Builds the `$sparkplug/certificates/...` topic on which a Sparkplug Aware
        # MQTT Server must make a retained copy of a BIRTH message available.
        #
        # The topic mirrors the original BIRTH topic but is prefixed and *includes
        # the namespace segment*:
        #
        #   NBIRTH spBv1.0/G1/NBIRTH/E1     -> $sparkplug/certificates/spBv1.0/G1/NBIRTH/E1
        #   DBIRTH spBv1.0/G1/DBIRTH/E1/D1  -> $sparkplug/certificates/spBv1.0/G1/DBIRTH/E1/D1
        def self.certificate_topic_for(parts : TopicParts) : String
          String.build do |str|
            str << CERTIFICATE_PREFIX
            str << parts.namespace << '/'
            str << parts.group_id << '/'
            str << parts.message_type << '/'
            str << parts.edge_node_id
            if device_id = parts.device_id
              str << '/' << device_id
            end
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
