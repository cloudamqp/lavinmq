require "../amqp"
require "./subscription_options"

module LavinMQ
  module MQTT
    EXCHANGE      = "mqtt.default"
    QOS_HEADER    = "mqtt.qos"
    RETAIN_HEADER = "mqtt.retain"
    # Binding arguments carrying the v5 subscription options that outlive the
    # SUBSCRIBE. Left out of the table entirely when false, so a default
    # subscription's arguments stay byte-identical to what LavinMQ has always
    # written and older definitions files keep loading unchanged.
    NO_LOCAL_HEADER            = "mqtt.no-local"
    RETAIN_AS_PUBLISHED_HEADER = "mqtt.retain-as-published"
    # Highest QoS LavinMQ supports, used to clamp granted and delivery QoS. At 2
    # the v5 CONNACK omits Maximum QoS, which may only be sent as 0 or 1
    # (3.2.2.3.4).
    MAX_QOS = 2u8
    # Queue argument carrying a session's Session Expiry Interval, in seconds.
    # It lives in the arguments because that is the only part of the
    # Queue::Declare frame definitions_store persists that can hold a UInt32.
    SESSION_EXPIRY_ARG = "x-mqtt-session-expiry"

    SESSION_PREFIX = "mqtt."

    # One per QoS level. Treat as read-only - they are shared by every default
    # subscription.
    QOS0_ARGUMENTS = AMQP::Table.new({QOS_HEADER => 0u8})
    QOS1_ARGUMENTS = AMQP::Table.new({QOS_HEADER => 1u8})
    QOS2_ARGUMENTS = AMQP::Table.new({QOS_HEADER => 2u8})

    # Stamped on every message replayed from the retain store at subscribe time,
    # which always carries RETAIN=1 [MQTT-3.3.1-8]. Shared and never mutated, so
    # a SUBSCRIBE allocates no table - and none at all when Retain Handling
    # suppresses the replay.
    RETAINED_HEADERS = AMQP::Table.new({RETAIN_HEADER => true})

    # The highest QoS we can actually deliver for a requested one, and the single
    # place that rule lives. Accepts anything: the packet hands us a `UInt8`, the
    # message store a `UInt8?`, and a binding table any `Int` at all, since
    # bindings made over the HTTP API or imported from a definitions file never
    # pass through the MQTT parser. A missing or negative value is QoS 0.
    def self.granted_qos(qos : Int?) : UInt8
      return 0u8 unless qos
      qos.clamp(0, MAX_QOS).to_u8
    end

    # A session's queue name.
    def self.session_name(client_id : String) : String
      "#{SESSION_PREFIX}#{client_id}"
    end

    # The binding arguments carrying a subscription's options.
    #
    # Returns one of the shared constants when neither option is set, which
    # is the overwhelmingly common case, so an ordinary subscription allocates
    # nothing. Only a subscription that actually uses an option builds a table.
    def self.subscription_arguments(options : SubscriptionOptions) : AMQP::Table
      qos = options.qos
      unless options.no_local? || options.retain_as_published?
        case qos
        when 0u8 then return QOS0_ARGUMENTS
        when 1u8 then return QOS1_ARGUMENTS
        else          return QOS2_ARGUMENTS
        end
      end
      arguments = AMQP::Table.new({QOS_HEADER => qos})
      arguments[NO_LOCAL_HEADER] = true if options.no_local?
      arguments[RETAIN_AS_PUBLISHED_HEADER] = true if options.retain_as_published?
      arguments
    end

    # The options carried in binding arguments, the inverse of
    # `subscription_arguments`.
    #
    # Hostile-input safe, and it has to be: `Exchange#bind` runs this during
    # `load!`, so a raise here is a boot failure. Any integer type is accepted
    # for the QoS and clamped; a boolean key that is not exactly `true` - a
    # string, an int, absent - reads as false. Unlike `Session.expiry_from`
    # nothing is lost by degrading quietly, so it does not warn.
    def self.subscription_options(arguments : AMQP::Table?) : SubscriptionOptions
      return SubscriptionOptions.new unless arguments
      SubscriptionOptions.new(
        granted_qos(arguments[QOS_HEADER]?.as?(Int)),
        arguments[NO_LOCAL_HEADER]? == true,
        arguments[RETAIN_AS_PUBLISHED_HEADER]? == true)
    end
  end
end
