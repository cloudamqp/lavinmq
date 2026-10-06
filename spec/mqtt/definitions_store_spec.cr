require "../spec_helper"

# Declared the way the MQTT broker and the definitions importer do it, as queues
# of type "mqtt" and bindings from the MQTT exchange.
private def declare_mqtt_session(vhost, name, clean_session = false)
  vhost.declare_queue(name, !clean_session, clean_session, LavinMQ::MQTT::Session::ARGUMENTS)
  vhost.session(name)
end

private def subscribe_mqtt_session(vhost, name, topic_filter, qos)
  vhost.bind_queue(name, LavinMQ::MQTT::EXCHANGE, topic_filter, LavinMQ::MQTT.qos_arguments(qos))
end

describe LavinMQ::MQTT::DefinitionsStore do
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

  it "drops all subscriptions of a deleted session, also after a restart" do
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

      restart_server(s)

      v = s.vhosts["/"]
      v.session?("mqtt.gone").should be_nil
      v.mqtt_exchange.bindings_details.map(&.routing_key).should eq ["e/f"]
    end
  end

  it "restores durable sessions and their subscriptions after a compaction and restart" do
    with_amqp_server do |s|
      LavinMQ::Config.instance.max_deleted_definitions = 4
      v = s.vhosts["/"]
      declare_mqtt_session(v, "mqtt.durable")
      subscribe_mqtt_session(v, "mqtt.durable", "a/b", 1u8)
      subscribe_mqtt_session(v, "mqtt.durable", "c/#", 0u8)
      # A clean session is transient: neither it nor its subscription persists
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
      v.queue?("mqtt.durable").should be_nil
      v.session?("mqtt.clean").should be_nil
      subscriptions = v.session_subscriptions(v.session("mqtt.durable"))
      subscriptions.map(&.routing_key).sort!.should eq ["a/b", "c/#"]
      subscriptions.find! { |sub| sub.routing_key == "a/b" }.binding_key.qos.should eq 1u8
      subscriptions.find! { |sub| sub.routing_key == "c/#" }.binding_key.qos.should eq 0u8
    end
  end
end
