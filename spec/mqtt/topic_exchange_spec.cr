require "./spec_helper"
require "../../src/lavinmq/definitions"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers

  # Publishes over MQTT at QoS 1, so the PubAck proves the server finished
  # routing the message before we assert on the AMQP side.
  def self.mqtt_publish(server, topic, payload = "data", retain = false)
    with_client_io(server) do |io|
      connect(io, client_id: "pub")
      publish(io, topic: topic, qos: 1u8, payload: payload.to_slice, retain: retain)
      disconnect(io)
    end
  end

  describe LavinMQ::AMQP::MqttTopicExchange do
    it "routes an MQTT publish to a queue bound with an MQTT topic filter" do
      with_server do |server|
        with_channel(server) do |ch|
          x = ch.exchange("mqtt_topic_spec", "x-mqtt-topic")
          q = ch.queue("amqp_q")
          q.bind(x.name, "a/b/#")

          mqtt_publish(server, "a/b/c")

          msg = q.get(no_ack: true).should_not be_nil
          msg.exchange.should eq "mqtt_topic_spec"
          msg.routing_key.should eq "a/b/c"
          msg.properties.delivery_mode.should eq 2
          msg.body_io.to_s.should eq "data"
        end
      end
    end

    it "doesn't route a topic that no binding matches" do
      with_server do |server|
        with_channel(server) do |ch|
          x = ch.exchange("mqtt_topic_spec", "x-mqtt-topic")
          q = ch.queue("amqp_q")
          q.bind(x.name, "a/b/+")

          mqtt_publish(server, "a/b/c/d")

          server.vhosts["/"].queue("amqp_q").message_count.should eq 0
        end
      end
    end

    # `#` includes the parent level [MQTT-4.7.1-2].
    it "routes the parent topic of a # filter" do
      with_server do |server|
        with_channel(server) do |ch|
          x = ch.exchange("mqtt_topic_spec", "x-mqtt-topic")
          q = ch.queue("amqp_q")
          q.bind(x.name, "a/#")

          mqtt_publish(server, "a")

          msg = q.get(no_ack: true).should_not be_nil
          msg.routing_key.should eq "a"
        end
      end
    end

    it "gives every queue bound to the same filter a copy" do
      with_server do |server|
        with_channel(server) do |ch|
          x = ch.exchange("mqtt_topic_spec", "x-mqtt-topic")
          q1 = ch.queue("amqp_q1")
          q2 = ch.queue("amqp_q2")
          q1.bind(x.name, "a/#")
          q2.bind(x.name, "a/#")

          mqtt_publish(server, "a/b")

          q1.get(no_ack: true).should_not be_nil
          q2.get(no_ack: true).should_not be_nil
        end
      end
    end

    # One tree entry per filter, so an unbind may only unsubscribe once the
    # filter has no destinations left.
    it "keeps a filter subscribed while another destination is bound to it" do
      with_server do |server|
        with_channel(server) do |ch|
          x = ch.exchange("mqtt_topic_spec", "x-mqtt-topic")
          q1 = ch.queue("amqp_q1")
          q2 = ch.queue("amqp_q2")
          q1.bind(x.name, "a/#")
          q2.bind(x.name, "a/#")

          q1.unbind(x.name, "a/#")

          mqtt_publish(server, "a/b")
          server.vhosts["/"].queue("amqp_q1").message_count.should eq 0
          q2.get(no_ack: true).should_not be_nil
        end
      end
    end

    # The tree holds one entry per filter per exchange, so unbinding the last
    # binding of a filter unsubscribes - but only that exchange's entry.
    it "keeps other exchanges' subscriptions when one unbinds the same filter" do
      with_server do |server|
        with_channel(server) do |ch|
          x1 = ch.exchange("mqtt_topic_one", "x-mqtt-topic")
          x2 = ch.exchange("mqtt_topic_two", "x-mqtt-topic")
          q1 = ch.queue("amqp_q1")
          q2 = ch.queue("amqp_q2")
          q1.bind(x1.name, "a/#")
          q2.bind(x2.name, "a/#")

          mqtt_publish(server, "a/b")
          q1.get(no_ack: true).should_not be_nil
          q2.get(no_ack: true).should_not be_nil

          q1.unbind(x1.name, "a/#")

          mqtt_publish(server, "a/b")
          server.vhosts["/"].queue("amqp_q1").message_count.should eq 0
          q2.get(no_ack: true).should_not be_nil
        end
      end
    end

    # Pinned decision, not an accident: the tree yields the exchange once per
    # matching filter and we don't deduplicate. See the ADR.
    it "delivers one copy per matching filter, not per queue" do
      with_server do |server|
        with_channel(server) do |ch|
          x = ch.exchange("mqtt_topic_spec", "x-mqtt-topic")
          q = ch.queue("amqp_q")
          q.bind(x.name, "sensors/#")
          q.bind(x.name, "sensors/+/temp")

          mqtt_publish(server, "sensors/a/temp")

          server.vhosts["/"].queue("amqp_q").message_count.should eq 2
        end
      end
    end

    it "stops routing when the bound queue is deleted" do
      with_server do |server|
        with_channel(server) do |ch|
          x = ch.exchange("mqtt_topic_spec", "x-mqtt-topic")
          q = ch.queue("amqp_q")
          q.bind(x.name, "a/#")
          q.delete

          server.vhosts["/"].mqtt_subscriptions.any?("a/b").should be_false
          mqtt_publish(server, "a/b")
          server.vhosts["/"].exchange(x.name).binding_count.should eq 0
        end
      end
    end

    it "unsubscribes every filter when the exchange is deleted" do
      with_server do |server|
        with_channel(server) do |ch|
          x = ch.exchange("mqtt_topic_spec", "x-mqtt-topic")
          q = ch.queue("amqp_q")
          q.bind(x.name, "a/#")
          q.bind(x.name, "b/#")
          x.delete

          server.vhosts["/"].mqtt_subscriptions.empty?.should be_true
          mqtt_publish(server, "a/b")
          server.vhosts["/"].queue("amqp_q").message_count.should eq 0
        end
      end
    end

    it "auto deletes when its last binding is removed" do
      with_server do |server|
        with_channel(server) do |ch|
          x = ch.exchange("mqtt_topic_spec", "x-mqtt-topic", auto_delete: true)
          q = ch.queue("amqp_q")
          q.bind(x.name, "a/#")
          q.bind(x.name, "b/#")

          q.unbind(x.name, "a/#")
          server.vhosts["/"].exchange?(x.name).should_not be_nil

          q.unbind(x.name, "b/#")
          server.vhosts["/"].exchange?(x.name).should be_nil
          server.vhosts["/"].mqtt_subscriptions.empty?.should be_true
        end
      end
    end

    it "restores durable bindings from the definitions on restart" do
      with_server do |server|
        with_channel(server) do |ch|
          x = ch.exchange("mqtt_topic_spec", "x-mqtt-topic", durable: true)
          q = ch.queue("amqp_q", durable: true)
          q.bind(x.name, "a/#")
        end

        restart_server(server)

        vhost = server.vhosts["/"]
        vhost.exchange("mqtt_topic_spec").binding_count.should eq 1
        vhost.mqtt_subscriptions.any?("a/b").should be_true

        mqtt_publish(server, "a/b")
        vhost.queue("amqp_q").message_count.should eq 1
      end
    end

    it "refuses an AMQP publish" do
      with_server do |server|
        with_channel(server) do |ch|
          x = ch.exchange("mqtt_topic_spec", "x-mqtt-topic")
          expect_raises(AMQP::Client::Channel::ClosedException, /ACCESS_REFUSED/) do
            x.publish_confirm("data", "a/b")
          end
        end
      end
    end

    it "is exported to, and restored from, definitions" do
      with_server do |server|
        vhost = server.vhosts["/"]
        vhost.declare_exchange("mqtt_topic_spec", "x-mqtt-topic", true, false)
        vhost.declare_queue("amqp_q", true, false)
        vhost.bind_queue("amqp_q", "mqtt_topic_spec", "a/#")

        definitions = JSON.parse(String.build { |io| LavinMQ::VHostDefinitions.new(server, vhost).export(io) })
        exchange = definitions["exchanges"].as_a.find { |e| e["name"] == "mqtt_topic_spec" }.should_not be_nil
        exchange["type"].should eq "x-mqtt-topic"
        binding = definitions["bindings"].as_a.find { |b| b["source"] == "mqtt_topic_spec" }.should_not be_nil
        binding["routing_key"].should eq "a/#"

        vhost.delete_exchange("mqtt_topic_spec")
        vhost.mqtt_subscriptions.empty?.should be_true

        LavinMQ::VHostDefinitions.new(server, vhost).import(definitions)
        vhost.exchange("mqtt_topic_spec").binding_count.should eq 1
        vhost.mqtt_subscriptions.any?("a/b").should be_true
      end
    end

    it "isn't counted among mqtt.default's bindings" do
      with_server do |server|
        vhost = server.vhosts["/"]
        default = vhost.exchange(LavinMQ::MQTT::EXCHANGE).as(LavinMQ::MQTT::Exchange)
        with_channel(server) do |ch|
          x = ch.exchange("mqtt_topic_spec", "x-mqtt-topic")
          ch.queue("amqp_q").bind(x.name, "a/#")

          default.binding_count.should eq 0
          default.bindings_details.should be_empty

          with_client_io(server) do |io|
            connect(io, client_id: "sub", clean_session: true)
            subscribe(io, topic_filters: [subtopic("a/b", 0u8)])

            default.binding_count.should eq 1
            default.bindings_details.map(&.routing_key).should eq ["a/b"]
            vhost.exchange(x.name).binding_count.should eq 1
            disconnect(io)
          end
        end
      end
    end

    it "delivers to MQTT sessions and AMQP queues from the same tree" do
      with_server do |server|
        with_channel(server) do |ch|
          x = ch.exchange("mqtt_topic_spec", "x-mqtt-topic")
          q = ch.queue("amqp_q")
          q.bind(x.name, "a/+")

          with_client_io(server) do |sub|
            connect(sub, client_id: "sub", clean_session: true)
            subscribe(sub, topic_filters: [subtopic("a/+", 1u8)])

            mqtt_publish(server, "a/b")

            packet = read_packet(sub).should be_a(MQTT::Protocol::Publish)
            packet.topic.should eq "a/b"
            q.get(no_ack: true).should_not be_nil
            disconnect(sub)
          end
        end
      end
    end

    # Wills take a different route into the exchange than a PUBLISH packet: the
    # broker publishes them from the client's teardown path.
    it "routes a will message" do
      with_server do |server|
        with_channel(server) do |ch|
          x = ch.exchange("mqtt_topic_spec", "x-mqtt-topic")
          q = ch.queue("amqp_q")
          q.bind(x.name, "will/#")

          with_client_io(server) do |io|
            will = MQTT::Protocol::Will.new(
              topic: "will/t", payload: "dead".to_slice, qos: 0u8, retain: false)
            # No Disconnect: closing the socket is what publishes the will.
            connect(io, client_id: "will_client", will: will, keepalive: 1u16)
          end

          msg = wait_for { q.get(no_ack: true) }.should_not be_nil
          msg.routing_key.should eq "will/t"
          msg.body_io.to_s.should eq "dead"
        end
      end
    end

    it "doesn't replay retained messages on bind" do
      with_server do |server|
        mqtt_publish(server, "a/b", retain: true)

        with_channel(server) do |ch|
          x = ch.exchange("mqtt_topic_spec", "x-mqtt-topic")
          q = ch.queue("amqp_q")
          q.bind(x.name, "a/#")

          server.vhosts["/"].queue("amqp_q").message_count.should eq 0

          mqtt_publish(server, "a/b")
          q.get(no_ack: true).should_not be_nil
        end
      end
    end
  end
end
