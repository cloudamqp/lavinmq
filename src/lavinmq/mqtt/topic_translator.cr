module LavinMQ
  module MQTT
    # Encapsulates the conversion between MQTT topic syntax and AMQP routing
    # key syntax for cross-protocol routing (see issue #1136).
    #
    # MQTT separates topic levels with '/', AMQP with '.'. The translation is a
    # length-preserving 1:1 character substitution. This is intentionally naive
    # and matches the behaviour of other brokers (e.g. RabbitMQ's MQTT plugin):
    # a literal '.' inside an MQTT topic level collapses into an extra AMQP
    # level (e.g. "v1.2/data" => "v1.2.data"). This is a documented limitation;
    # it keeps MQTT levels lined up with AMQP levels so that AMQP wildcard
    # bindings ('*'/'#') match as a user would expect.
    module TopicTranslator
      # Translates a concrete (wildcard-free) MQTT topic into an AMQP routing
      # key. Published MQTT topics never contain '+'/'#', so only the level
      # separator is rewritten.
      def self.mqtt_to_amqp(topic : String) : String
        topic.tr("/", ".")
      end
    end
  end
end
