require "../spec_helper"

# Declared the way the MQTT broker and the definitions importer do it, as queues
# of type "mqtt" and bindings from the MQTT exchange.
private def declare_mqtt_session(vhost, name, clean_session = false)
  vhost.declare_queue(name, !clean_session, clean_session, LavinMQ::MQTT::Session::ARGUMENTS)
  vhost.mqtt.session(name)
end

private def subscribe_mqtt_session(vhost, name, topic_filter, qos)
  vhost.bind_queue(name, LavinMQ::MQTT::EXCHANGE, topic_filter, LavinMQ::MQTT.qos_arguments(qos))
end

private def definitions_mqtt(vhost)
  File.join(vhost.data_dir, "definitions.mqtt")
end

describe LavinMQ::MQTT::DefinitionsStore do
  it "returns only the given session's subscriptions" do
    with_amqp_server do |s|
      v = s.vhosts["/"]
      declare_mqtt_session(v, "mqtt.one")
      declare_mqtt_session(v, "mqtt.two")
      subscribe_mqtt_session(v, "mqtt.one", "a/b", 0u8)
      subscribe_mqtt_session(v, "mqtt.two", "c/d", 0u8)

      v.mqtt.subscriptions(v.mqtt.session("mqtt.one")).map(&.routing_key).should eq ["a/b"]
      v.mqtt.subscriptions(v.mqtt.session("mqtt.two")).map(&.routing_key).should eq ["c/d"]
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
      v.mqtt.exchange.binding_count.should eq 4

      v.delete_queue("mqtt.gone")

      v.mqtt.session?("mqtt.gone").should be_nil
      v.mqtt.exchange.bindings_details.map(&.routing_key).should eq ["e/f"]

      restart_server(s)

      v = s.vhosts["/"]
      v.mqtt.session?("mqtt.gone").should be_nil
      v.mqtt.exchange.bindings_details.map(&.routing_key).should eq ["e/f"]
    end
  end

  it "reports a failed subscription for a session replaced under the same name" do
    with_amqp_server do |s|
      v = s.vhosts["/"]
      old = declare_mqtt_session(v, "mqtt.c")
      old.delete
      replacement = declare_mqtt_session(v, "mqtt.c")

      old.subscribe("a/b", 0u8).should be_false
      v.mqtt.subscriptions(replacement).should be_empty
    end
  end

  it "doesn't let a replaced session delete its replacement" do
    with_amqp_server do |s|
      v = s.vhosts["/"]
      old = declare_mqtt_session(v, "mqtt.c")
      # A delete through the API unregisters the session before deleting it,
      # and a client can declare a replacement in between
      v.mqtt.delete_session("mqtt.c")
      replacement = declare_mqtt_session(v, "mqtt.c")

      old.delete
      v.mqtt.session?("mqtt.c").should be replacement
    end
  end

  it "writes nothing for a subscribe or unsubscribe that changes nothing" do
    with_amqp_server do |s|
      v = s.vhosts["/"]
      session = declare_mqtt_session(v, "mqtt.sub")
      session.subscribe("a/b", 1u8).should be_true
      size = File.size(definitions_mqtt(v))

      session.subscribe("a/b", 1u8).should be_true
      session.unsubscribe("never/subscribed").should be_true
      File.size(definitions_mqtt(v)).should eq size
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
      # Deletes up to the compaction threshold
      (LavinMQ::Config.instance.max_deleted_definitions - 1).times do |i|
        declare_mqtt_session(v, "mqtt.churn#{i}")
        v.delete_queue("mqtt.churn#{i}")
      end
      size = File.size(definitions_mqtt(v))
      declare_mqtt_session(v, "mqtt.churn")
      v.delete_queue("mqtt.churn")
      File.size(definitions_mqtt(v)).should be < size

      restart_server(s)

      v = s.vhosts["/"]
      v.mqtt.sessions.map(&.name).should eq ["mqtt.durable"]
      v.queue?("mqtt.durable").should be_nil
      subscriptions = v.mqtt.subscriptions(v.mqtt.session("mqtt.durable"))
      subscriptions.map(&.routing_key).sort!.should eq ["a/b", "c/#"]
      subscriptions.find! { |sub| sub.routing_key == "a/b" }.binding_key.qos.should eq 1u8
      subscriptions.find! { |sub| sub.routing_key == "c/#" }.binding_key.qos.should eq 0u8
    end
  end

  it "compacts into a fresh file when a crashed compaction left one behind" do
    with_amqp_server do |s|
      LavinMQ::Config.instance.max_deleted_definitions = 1
      v = s.vhosts["/"]
      declare_mqtt_session(v, "mqtt.sub").subscribe("a/b", 1u8)
      File.write("#{definitions_mqtt(v)}.tmp", "left by a crashed compaction")

      declare_mqtt_session(v, "mqtt.churn")
      v.delete_queue("mqtt.churn")
      restart_server(s)

      v = s.vhosts["/"]
      v.mqtt.subscriptions(v.mqtt.session("mqtt.sub")).map(&.routing_key).should eq ["a/b"]
    end
  end

  # A crash mid-append can leave part of a record at the end of the file
  it "keeps the records appended after a partial one" do
    with_amqp_server do |s|
      v = s.vhosts["/"]
      declare_mqtt_session(v, "mqtt.sub").subscribe("a/b", 1u8)
      record = LavinMQ::MQTT::DefinitionsFormat.subscription_record(
        LavinMQ::MQTT::DefinitionsFormat::Op::Subscribe, "mqtt.sub", "torn", 1u8)
      File.open(definitions_mqtt(v), "a") { |f| f.write record[0, 7] }

      restart_server(s)
      v = s.vhosts["/"]
      v.mqtt.session("mqtt.sub").subscribe("c/d", 1u8)
      restart_server(s)

      v = s.vhosts["/"]
      v.mqtt.subscriptions(v.mqtt.session("mqtt.sub")).map(&.routing_key).sort!.should eq ["a/b", "c/d"]
    end
  end
end
