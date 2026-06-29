require "./spec_helper"

module MqttSpecs
  extend MqttHelpers

  describe "MQTT exchange to AMQP exchange binding (#1136)" do
    it "routes an MQTT publish into a bound AMQP topic exchange with a translated routing key" do
      with_server do |server|
        vhost = server.vhosts["/"]
        vhost.declare_exchange("amq.dest", "topic", false, false)
        vhost.declare_queue("dest-q", false, false)
        vhost.bind_queue("dest-q", "amq.dest", "sensors.#")
        # Bind the MQTT exchange -> AMQP exchange using an MQTT-syntax filter.
        vhost.bind_exchange("amq.dest", LavinMQ::MQTT::EXCHANGE, "sensors/+/temp")

        with_client_io(server) do |io|
          connect(io)
          publish(io, topic: "sensors/dev1/temp", payload: "25".to_slice, qos: 0u8)
        end

        q = vhost.queue("dest-q")
        wait_for { q.message_count == 1 }
        q.message_count.should eq 1
        q.basic_get(true) do |env|
          env.message.routing_key.should eq "sensors.dev1.temp"
          String.new(env.message.body).should eq "25"
        end.should be_true
      end
    end

    it "maps MQTT QoS>=1 to a persistent AMQP delivery_mode" do
      with_server do |server|
        vhost = server.vhosts["/"]
        vhost.declare_exchange("amq.persist", "topic", true, false)
        vhost.declare_queue("persist-q", true, false)
        vhost.bind_queue("persist-q", "amq.persist", "#")
        vhost.bind_exchange("amq.persist", LavinMQ::MQTT::EXCHANGE, "sensors/+/temp")

        with_client_io(server) do |io|
          connect(io)
          publish(io, topic: "sensors/dev1/temp", payload: "25".to_slice, qos: 1u8)
        end

        q = vhost.queue("persist-q")
        wait_for { q.message_count == 1 }
        q.basic_get(true) do |env|
          env.message.properties.delivery_mode.should eq 2u8
        end.should be_true
      end
    end

    it "maps MQTT QoS 0 to a transient AMQP delivery_mode" do
      with_server do |server|
        vhost = server.vhosts["/"]
        vhost.declare_exchange("amq.transient-dm", "topic", true, false)
        vhost.declare_queue("transient-dm-q", true, false)
        vhost.bind_queue("transient-dm-q", "amq.transient-dm", "#")
        vhost.bind_exchange("amq.transient-dm", LavinMQ::MQTT::EXCHANGE, "sensors/+/temp")

        with_client_io(server) do |io|
          connect(io)
          publish(io, topic: "sensors/dev1/temp", payload: "25".to_slice, qos: 0u8)
        end

        q = vhost.queue("transient-dm-q")
        wait_for { q.message_count == 1 }
        q.basic_get(true) do |env|
          env.message.properties.delivery_mode.should eq 1u8
        end.should be_true
      end
    end

    it "does not route an MQTT publish that doesn't match the binding filter" do
      with_server do |server|
        vhost = server.vhosts["/"]
        vhost.declare_exchange("amq.dest2", "topic", false, false)
        vhost.declare_queue("dest-q2", false, false)
        vhost.bind_queue("dest-q2", "amq.dest2", "#")
        vhost.bind_exchange("amq.dest2", LavinMQ::MQTT::EXCHANGE, "sensors/+/temp")

        with_client_io(server) do |io|
          connect(io)
          publish(io, topic: "other/topic", payload: "x".to_slice, qos: 0u8)
        end

        q = vhost.queue("dest-q2")
        sleep 50.milliseconds
        q.message_count.should eq 0
      end
    end

    it "reports the bound AMQP exchange as the binding destination" do
      with_server do |server|
        vhost = server.vhosts["/"]
        vhost.declare_exchange("amq.dest3", "topic", false, false)
        vhost.bind_exchange("amq.dest3", LavinMQ::MQTT::EXCHANGE, "a/b")
        exchange = vhost.exchange(LavinMQ::MQTT::EXCHANGE).as(LavinMQ::MQTT::Exchange)
        bindings = exchange.bindings_details
        bindings.size.should eq 1
        bindings.first.destination.name.should eq "amq.dest3"
        bindings.first.routing_key.should eq "a/b"
      end
    end

    it "preserves binding arguments on a cross-protocol binding" do
      with_server do |server|
        vhost = server.vhosts["/"]
        vhost.declare_exchange("amq.args", "topic", false, false)
        args = LavinMQ::AMQP::Table.new({"x-foo" => "bar"})
        vhost.bind_exchange("amq.args", LavinMQ::MQTT::EXCHANGE, "a/b", args)
        exchange = vhost.exchange(LavinMQ::MQTT::EXCHANGE).as(LavinMQ::MQTT::Exchange)
        exchange.bindings_details.first.arguments.should eq args
      end
    end

    it "removes the binding when unbound" do
      with_server do |server|
        vhost = server.vhosts["/"]
        vhost.declare_exchange("amq.dest4", "topic", false, false)
        vhost.bind_exchange("amq.dest4", LavinMQ::MQTT::EXCHANGE, "a/b")
        exchange = vhost.exchange(LavinMQ::MQTT::EXCHANGE).as(LavinMQ::MQTT::Exchange)
        exchange.bindings_details.size.should eq 1
        vhost.unbind_exchange("amq.dest4", LavinMQ::MQTT::EXCHANGE, "a/b")
        exchange.bindings_details.should be_empty
      end
    end
  end
end
