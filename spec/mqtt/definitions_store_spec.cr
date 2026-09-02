require "../spec_helper"

MQTT_QUEUE_ARGS = LavinMQ::AMQP::Table.new({"x-queue-type" => "mqtt"})

# Sessions and subscriptions are declared the way the MQTT broker and the
# definitions importer do it, as queues of type "mqtt" and bindings from the
# MQTT exchange.
def declare_mqtt_session(vhost, name, clean_session = false)
  vhost.declare_queue(name, !clean_session, clean_session, MQTT_QUEUE_ARGS)
  vhost.session(name)
end

def subscribe_mqtt_session(vhost, name, topic_filter, qos)
  vhost.bind_queue(name, LavinMQ::MQTT::EXCHANGE, topic_filter,
    LavinMQ::AMQP::Table.new({LavinMQ::MQTT::QOS_HEADER => qos}))
end

describe LavinMQ::MQTT::DefinitionsStore do
  it "holds sessions apart from queues" do
    with_amqp_server do |s|
      v = s.vhosts["/"]
      session = declare_mqtt_session(v, "mqtt.sub")
      session.should be_a LavinMQ::MQTT::Session
      v.session?("mqtt.sub").should be session
      v.queue?("mqtt.sub").should be_nil
      v.sessions_size.should eq 1
    end
  end

  it "keeps a subscription in the exchange's subscription tree" do
    with_amqp_server do |s|
      v = s.vhosts["/"]
      declare_mqtt_session(v, "mqtt.sub")
      subscribe_mqtt_session(v, "mqtt.sub", "a/b", 1u8)

      v.mqtt_exchange.binding_count.should eq 1
      subscription = v.session_subscriptions(v.session("mqtt.sub")).first
      subscription.routing_key.should eq "a/b"
      subscription.binding_key.qos.should eq 1u8

      v.unbind_queue("mqtt.sub", LavinMQ::MQTT::EXCHANGE, "a/b", LavinMQ::MQTT::QOS1_ARGUMENTS)
      v.mqtt_exchange.binding_count.should eq 0
      v.session_subscriptions(v.session("mqtt.sub")).should be_empty
    end
  end

  it "returns only the given session's subscriptions" do
    with_amqp_server do |s|
      v = s.vhosts["/"]
      declare_mqtt_session(v, "mqtt.one")
      declare_mqtt_session(v, "mqtt.two")
      subscribe_mqtt_session(v, "mqtt.one", "a/b", 0u8)
      subscribe_mqtt_session(v, "mqtt.two", "c/d", 0u8)

      v.session_subscriptions(v.session("mqtt.one")).map(&.routing_key).should eq ["a/b"]
      v.session_subscriptions(v.session("mqtt.two")).map(&.routing_key).should eq ["c/d"]
    end
  end

  it "drops all subscriptions of a deleted session" do
    with_amqp_server do |s|
      v = s.vhosts["/"]
      declare_mqtt_session(v, "mqtt.gone")
      declare_mqtt_session(v, "mqtt.stays")
      subscribe_mqtt_session(v, "mqtt.gone", "a/b", 0u8)
      subscribe_mqtt_session(v, "mqtt.gone", "c/+", 0u8)
      subscribe_mqtt_session(v, "mqtt.gone", "d/#", 1u8)
      subscribe_mqtt_session(v, "mqtt.stays", "e/f", 0u8)
      v.mqtt_exchange.binding_count.should eq 4

      v.delete_queue("mqtt.gone")

      v.session?("mqtt.gone").should be_nil
      v.mqtt_exchange.bindings_details.map(&.routing_key).should eq ["e/f"]
    end
  end

  it "replaces a subscription made again with another qos" do
    with_amqp_server do |s|
      v = s.vhosts["/"]
      session = declare_mqtt_session(v, "mqtt.sub")
      session.subscribe("a/b", 0u8)
      session.subscribe("a/b", 1u8)

      subscriptions = v.session_subscriptions(session)
      subscriptions.size.should eq 1
      subscriptions.first.binding_key.qos.should eq 1u8
    end
  end

  it "reports whether a subscription was established" do
    with_amqp_server do |s|
      v = s.vhosts["/"]
      session = declare_mqtt_session(v, "mqtt.sub")
      session.subscribe("a/b", 0u8).should be_true # new
      session.subscribe("a/b", 0u8).should be_true # already subscribed at this qos
      session.subscribe("a/b", 1u8).should be_true # qos replaced
      v.session_subscriptions(session).size.should eq 1
    end
  end

  # The race this guards: a clean-session client reconnecting under the same
  # client_id deletes the session from another fiber, so a subscribe can find
  # its session gone. The client must be told, or it waits forever on a topic
  # it was told it had subscribed to.
  it "reports a failed subscription for a session that has been deleted" do
    with_amqp_server do |s|
      v = s.vhosts["/"]
      session = declare_mqtt_session(v, "mqtt.gone")
      session.delete
      v.session?("mqtt.gone").should be_nil

      session.subscribe("a/b", 0u8).should be_false
      v.mqtt_exchange.binding_count.should eq 0
    end
  end

  it "grants the subscribed qos in the SubAck on success" do
    with_amqp_server do |s|
      v = s.vhosts["/"]
      broker = s.mqtt_server.broker("/")
      session = declare_mqtt_session(v, "mqtt.sub")

      tf = MQTT::Protocol::Subscribe::TopicFilter.new("a/b", 1u8)
      broker.grant(session, tf).should eq MQTT::Protocol::SubAck::ReturnCode::QoS1
      v.session_subscriptions(session).map(&.routing_key).should eq ["a/b"]
    end
  end

  it "grants Failure in the SubAck for a subscription it could not establish" do
    with_amqp_server do |s|
      v = s.vhosts["/"]
      broker = s.mqtt_server.broker("/")
      session = declare_mqtt_session(v, "mqtt.gone")
      session.delete

      tf = MQTT::Protocol::Subscribe::TopicFilter.new("a/b", 0u8)
      broker.grant(session, tf).should eq MQTT::Protocol::SubAck::ReturnCode::Failure
    end
  end

  it "restores durable sessions and their subscriptions after a compaction and restart" do
    with_amqp_server do |s|
      LavinMQ::Config.instance.max_deleted_definitions = 4
      v = s.vhosts["/"]
      declare_mqtt_session(v, "mqtt.durable")
      subscribe_mqtt_session(v, "mqtt.durable", "a/b", 1u8)
      subscribe_mqtt_session(v, "mqtt.durable", "c/#", 0u8)
      # A clean session is transient: neither it nor its subscription is persisted
      declare_mqtt_session(v, "mqtt.clean", clean_session: true)
      subscribe_mqtt_session(v, "mqtt.clean", "e/f", 0u8)
      # Trip the compaction threshold
      LavinMQ::Config.instance.max_deleted_definitions.times do
        v.declare_queue("q", true, false)
        v.delete_queue("q")
      end

      restart_server(s)

      v = s.vhosts["/"]
      v.session?("mqtt.durable").should_not be_nil
      v.session?("mqtt.clean").should be_nil
      subscriptions = v.session_subscriptions(v.session("mqtt.durable"))
      subscriptions.map(&.routing_key).sort!.should eq ["a/b", "c/#"]
      subscriptions.find! { |sub| sub.routing_key == "a/b" }.binding_key.qos.should eq 1u8
      subscriptions.find! { |sub| sub.routing_key == "c/#" }.binding_key.qos.should eq 0u8
    end
  end
end
