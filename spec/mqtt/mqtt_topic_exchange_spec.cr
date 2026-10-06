require "./spec_helper"

module MqttSpecs
  extend MqttHelpers

  describe "x-mqtt-topic exchange" do
    it "routes an MQTT publish to a queue bound with an MQTT topic filter" do
      with_server do |server|
        vhost = server.vhosts["/"]
        vhost.declare_exchange("xmqtt", "x-mqtt-topic", true, false)
        vhost.declare_queue("q1", true, false)
        vhost.bind_queue("q1", "xmqtt", "a/b/#")

        with_client_io(server) do |io|
          connect(io)
          publish(io, topic: "a/b/c", payload: "hello".to_slice, qos: 0u8)
          disconnect(io)
        end

        queue = vhost.queue("q1")
        wait_for { queue.message_count == 1 }
        queue.basic_get(no_ack: true) do |env|
          env.message.exchange_name.should eq "xmqtt"
          env.message.routing_key.should eq "a/b/c"
          env.message.properties.delivery_mode.should eq 2u8
          String.new(env.message.body).should eq "hello"
        end.should be_true
      end
    end

    it "does not route a non-matching topic" do
      with_server do |server|
        vhost = server.vhosts["/"]
        vhost.declare_exchange("xmqtt", "x-mqtt-topic", true, false)
        vhost.declare_queue("q1", true, false)
        vhost.bind_queue("q1", "xmqtt", "a/+/c")

        with_client_io(server) do |io|
          connect(io)
          publish(io, topic: "a/b/x", payload: "nope".to_slice, qos: 0u8)
          publish(io, topic: "a/b/c", payload: "yes".to_slice, qos: 0u8)
          disconnect(io)
        end

        queue = vhost.queue("q1")
        wait_for { queue.message_count == 1 }
        queue.basic_get(no_ack: true) do |env|
          String.new(env.message.body).should eq "yes"
        end.should be_true
      end
    end

    it "delivers a copy to each queue bound with the same filter" do
      with_server do |server|
        vhost = server.vhosts["/"]
        vhost.declare_exchange("xmqtt", "x-mqtt-topic", true, false)
        vhost.declare_queue("q1", true, false)
        vhost.declare_queue("q2", true, false)
        vhost.bind_queue("q1", "xmqtt", "a/#")
        vhost.bind_queue("q2", "xmqtt", "a/#")

        with_client_io(server) do |io|
          connect(io)
          publish(io, topic: "a/b", payload: "copy".to_slice, qos: 0u8)
          disconnect(io)
        end

        wait_for { vhost.queue("q1").message_count == 1 }
        wait_for { vhost.queue("q2").message_count == 1 }
      end
    end

    it "keeps one exchange's subscription when another unbinds the same filter" do
      with_server do |server|
        vhost = server.vhosts["/"]
        vhost.declare_exchange("x1", "x-mqtt-topic", true, false)
        vhost.declare_exchange("x2", "x-mqtt-topic", true, false)
        vhost.declare_queue("q1", true, false)
        vhost.declare_queue("q2", true, false)
        vhost.bind_queue("q1", "x1", "a/#")
        vhost.bind_queue("q2", "x2", "a/#")

        with_client_io(server) do |io|
          connect(io)
          publish(io, topic: "a/b", payload: "both".to_slice, qos: 0u8)
          wait_for { vhost.queue("q1").message_count == 1 }
          wait_for { vhost.queue("q2").message_count == 1 }

          vhost.unbind_queue("q2", "x2", "a/#", LavinMQ::AMQP::Table.new)
          publish(io, topic: "a/b", payload: "one".to_slice, qos: 0u8)
          wait_for { vhost.queue("q1").message_count == 2 }
          vhost.queue("q2").message_count.should eq 1
          disconnect(io)
        end
      end
    end

    it "delivers one copy per matching binding when filters overlap" do
      with_server do |server|
        vhost = server.vhosts["/"]
        vhost.declare_exchange("xmqtt", "x-mqtt-topic", true, false)
        vhost.declare_queue("q1", true, false)
        vhost.bind_queue("q1", "xmqtt", "sensors/#")
        vhost.bind_queue("q1", "xmqtt", "sensors/+/temp")

        with_client_io(server) do |io|
          connect(io)
          publish(io, topic: "sensors/a/temp", payload: "t".to_slice, qos: 0u8)
          disconnect(io)
        end

        wait_for { vhost.queue("q1").message_count == 2 }
      end
    end

    it "stops routing when the exchange is deleted" do
      with_server do |server|
        vhost = server.vhosts["/"]
        vhost.declare_exchange("xmqtt", "x-mqtt-topic", true, false)
        vhost.declare_queue("q1", true, false)
        vhost.bind_queue("q1", "xmqtt", "a/#")
        vhost.delete_exchange("xmqtt")
        vhost.mqtt_subscription_tree.empty?.should be_true

        with_client_io(server) do |io|
          connect(io)
          publish(io, topic: "a/b", payload: "lost".to_slice, qos: 1u8)
          disconnect(io)
        end
        vhost.queue("q1").message_count.should eq 0
      end
    end

    it "stops routing when the bound queue is deleted" do
      with_server do |server|
        vhost = server.vhosts["/"]
        vhost.declare_exchange("xmqtt", "x-mqtt-topic", true, false)
        vhost.declare_queue("q1", true, false)
        vhost.bind_queue("q1", "xmqtt", "a/#")
        vhost.delete_queue("q1")

        exchange = vhost.exchange("xmqtt")
        exchange.binding_count.should eq 0
        vhost.mqtt_subscription_tree.empty?.should be_true
      end
    end

    it "deletes an auto_delete exchange when its last binding is removed" do
      with_server do |server|
        vhost = server.vhosts["/"]
        vhost.declare_exchange("xad", "x-mqtt-topic", false, true)
        vhost.declare_queue("q1", true, false)
        vhost.bind_queue("q1", "xad", "a/#")
        vhost.unbind_queue("q1", "xad", "a/#", LavinMQ::AMQP::Table.new)
        vhost.exchange?("xad").should be_nil
        vhost.mqtt_subscription_tree.empty?.should be_true
      end
    end

    it "restores durable bindings through definitions replay after a restart" do
      with_server(clean_dir: false) do |server|
        vhost = server.vhosts["/"]
        vhost.declare_exchange("xmqtt", "x-mqtt-topic", true, false)
        vhost.declare_queue("q1", true, false)
        vhost.bind_queue("q1", "xmqtt", "a/b/#")
      end

      with_server(clean_dir: true) do |server|
        vhost = server.vhosts["/"]
        vhost.exchange("xmqtt").binding_count.should eq 1

        with_client_io(server) do |io|
          connect(io)
          publish(io, topic: "a/b/c", payload: "again".to_slice, qos: 0u8)
          disconnect(io)
        end
        wait_for { vhost.queue("q1").message_count == 1 }
      end
    end

    it "refuses basic.publish into the exchange" do
      with_server do |server|
        vhost = server.vhosts["/"]
        vhost.declare_exchange("xmqtt", "x-mqtt-topic", true, false)
        with_channel(server) do |ch|
          expect_raises(AMQP::Client::Channel::ClosedException, /ACCESS_REFUSED/) do
            ch.basic_publish_confirm("nope", "xmqtt", "a/b")
          end
        end
      end
    end

    # Pins the decision to not guard Exchange.Bind on internal exchanges:
    # a guard breaks federation, which binds internal exchanges as destinations.
    it "allows exchange.bind with the exchange as source and as destination" do
      with_server do |server|
        vhost = server.vhosts["/"]
        vhost.declare_exchange("xmqtt", "x-mqtt-topic", true, false)
        with_channel(server) do |ch|
          ch.exchange_bind("xmqtt", "amq.topic", "a/#") # exchange as source
          ch.exchange_bind("amq.topic", "xmqtt", "a.b") # exchange as destination
        end
        vhost.exchange("xmqtt").binding_count.should eq 1
      end
    end

    it "routes into a bound exchange and onwards with normal AMQP routing" do
      with_server do |server|
        vhost = server.vhosts["/"]
        vhost.declare_exchange("xmqtt", "x-mqtt-topic", true, false)
        vhost.declare_queue("q1", true, false)
        vhost.exchange("xmqtt").bind(vhost.exchange("amq.topic").as(LavinMQ::AMQP::Exchange), "a/#", nil)
        # the routing key stays the MQTT topic verbatim: no dots, so only an
        # exact literal binding key matches on an AMQP topic exchange
        vhost.bind_queue("q1", "amq.topic", "a/b/c")

        with_client_io(server) do |io|
          connect(io)
          publish(io, topic: "a/b/c", payload: "via amq.topic".to_slice, qos: 0u8)
          disconnect(io)
        end

        queue = vhost.queue("q1")
        wait_for { queue.message_count == 1 }
        queue.basic_get(no_ack: true) do |env|
          env.message.routing_key.should eq "a/b/c"
        end.should be_true
      end
    end

    it "keeps x-mqtt-topic bindings out of mqtt.default's binding view" do
      with_server do |server|
        vhost = server.vhosts["/"]
        vhost.declare_exchange("xmqtt", "x-mqtt-topic", true, false)
        vhost.declare_queue("q1", true, false)
        vhost.bind_queue("q1", "xmqtt", "a/b/#")

        with_client_io(server) do |io|
          connect(io, client_id: "sub")
          subscribe(io, topic_filters: [subtopic("a/+/c", 0u8)])

          mqtt_default = vhost.mqtt_exchange
          mqtt_default.binding_count.should eq 1
          details = mqtt_default.bindings_details
          details.size.should eq 1
          details.first.routing_key.should eq "a/+/c"

          vhost.exchange("xmqtt").binding_count.should eq 1
          disconnect(io)
        end
      end
    end

    it "still delivers to MQTT sessions sharing the tree" do
      with_server do |server|
        vhost = server.vhosts["/"]
        vhost.declare_exchange("xmqtt", "x-mqtt-topic", true, false)
        vhost.declare_queue("q1", true, false)
        vhost.bind_queue("q1", "xmqtt", "a/b/#")

        with_client_io(server) do |sub_io|
          connect(sub_io, client_id: "sub")
          subscribe(sub_io, topic_filters: [subtopic("a/+/c", 0u8)])

          with_client_io(server) do |pub_io|
            connect(pub_io, client_id: "pub")
            publish(pub_io, topic: "a/b/c", payload: "both".to_slice, qos: 0u8)
            disconnect(pub_io)
          end

          packet = read_publish(sub_io)
          packet.topic.should eq "a/b/c"
          String.new(packet.payload).should eq "both"
          disconnect(sub_io)
        end

        wait_for { vhost.queue("q1").message_count == 1 }
      end
    end

    it "does not replay retained messages on bind" do
      with_server do |server|
        vhost = server.vhosts["/"]
        with_client_io(server) do |io|
          connect(io)
          publish(io, topic: "a/b/c", payload: "retained".to_slice, qos: 0u8, retain: true)
          disconnect(io)
        end

        vhost.declare_exchange("xmqtt", "x-mqtt-topic", true, false)
        vhost.declare_queue("q1", true, false)
        vhost.bind_queue("q1", "xmqtt", "a/b/#")
        vhost.queue("q1").message_count.should eq 0

        with_client_io(server) do |io|
          connect(io)
          publish(io, topic: "a/b/c", payload: "live".to_slice, qos: 0u8)
          disconnect(io)
        end
        queue = vhost.queue("q1")
        wait_for { queue.message_count == 1 }
        queue.basic_get(no_ack: true) do |env|
          String.new(env.message.body).should eq "live"
        end.should be_true
      end
    end
  end
end
