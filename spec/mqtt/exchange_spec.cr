require "./spec_helper"

module MqttSpecs
  extend MqttHelpers

  describe LavinMQ::MQTT::Exchange do
    it "removes all subscriptions from the subscription tree when a session is removed" do
      with_server do |server|
        exchange = server.vhosts["/"].exchange(LavinMQ::MQTT::EXCHANGE).as(LavinMQ::MQTT::Exchange)
        with_client_io(server) do |io|
          connect(io, client_id: "sub", clean_session: true)
          subscribe(io, topic_filters: [
            subtopic("a/b", 0u8),
            subtopic("c/+", 0u8),
            subtopic("d/#", 0u8),
          ])
          exchange.bindings_details.size.should eq 3
          disconnect(io)
        end
        wait_for { exchange.bindings_details.empty? }
        exchange.bindings_details.should be_empty
      end
    end

    it "grants a QoS 2 subscription bound from outside the MQTT protocol as QoS 1" do
      with_server do |server|
        vhost = server.vhosts["/"]
        exchange = vhost.mqtt_exchange
        vhost.declare_queue("mqtt.sub", true, false,
          LavinMQ::AMQP::Table.new({"x-queue-type" => "mqtt"}))

        # Bindings made over the HTTP API or imported from a definitions file don't
        # pass through the MQTT parser, so the QoS header is any integer here.
        vhost.bind_queue("mqtt.sub", LavinMQ::MQTT::EXCHANGE, "a/b",
          LavinMQ::AMQP::Table.new({LavinMQ::MQTT::QOS_HEADER => 2}))

        binding = exchange.bindings_details.first
        binding.binding_key.qos.should eq 1u8
        binding.arguments.should eq LavinMQ::MQTT::QOS1_ARGUMENTS

        # bind and unbind must read the QoS the same way, or the unbind wouldn't
        # match the binding it's meant to remove
        vhost.unbind_queue("mqtt.sub", LavinMQ::MQTT::EXCHANGE, "a/b",
          LavinMQ::AMQP::Table.new({LavinMQ::MQTT::QOS_HEADER => 2}))
        exchange.bindings_details.should be_empty
      end
    end

    # Regression: `bind` and `unbind` used to be a pair of overloads, one for
    # MQTT::Session and one for the `Destination` union that refuses. Since
    # `Destination` contains `MQTT::Session`, both matched a session and the
    # winner depended on the order the definitions were compiled in. Requiring
    # `mqtt/broker` before `server` (which an embedder does, and our own spec
    # helper happens not to) selected the refusing one, and every SUBSCRIBE
    # died with "Access refused to mqtt.default".
    #
    # Calling through a `Destination`-typed variable is what reproduces it:
    # that is the static type at the real call site in DefinitionsStore.
    it "binds a session reached through a Destination-typed reference" do
      with_server do |server|
        vhost = server.vhosts["/"]
        exchange = vhost.mqtt_exchange
        vhost.declare_queue("mqtt.sub", true, false,
          LavinMQ::AMQP::Table.new({"x-queue-type" => "mqtt"}))
        destination = vhost.session("mqtt.sub").as(LavinMQ::Destination)

        exchange.bind(destination, "a/b", LavinMQ::MQTT::QOS0_ARGUMENTS).should be_true
        exchange.bindings_details.size.should eq 1

        exchange.unbind(destination, "a/b", LavinMQ::MQTT::QOS0_ARGUMENTS).should be_true
        exchange.bindings_details.should be_empty
      end
    end

    it "refuses a destination that is not an MQTT session" do
      with_server do |server|
        vhost = server.vhosts["/"]
        exchange = vhost.mqtt_exchange
        vhost.declare_queue("plain", false, false)
        destination = vhost.queue("plain").as(LavinMQ::Destination)

        expect_raises(LavinMQ::Exchange::AccessRefused) do
          exchange.bind(destination, "a/b", nil)
        end
        expect_raises(LavinMQ::Exchange::AccessRefused) do
          exchange.unbind(destination, "a/b", nil)
        end
      end
    end

    it "exposes subscriptions as MQTT::SubscriptionDetails sharing the binding details interface" do
      with_server do |server|
        exchange = server.vhosts["/"].exchange(LavinMQ::MQTT::EXCHANGE).as(LavinMQ::MQTT::Exchange)
        with_client_io(server) do |io|
          connect(io, client_id: "sub", clean_session: true)
          subscribe(io, topic_filters: [subtopic("a/b", 0u8)])

          details = exchange.bindings_details
          details.size.should eq 1
          sd = details.first
          sd.should be_a(LavinMQ::MQTT::SubscriptionDetails)
          sd.binding_key.should be_a(LavinMQ::MQTT::SubscriptionKey)

          # Same NamedTuple shape as LavinMQ::AMQP::BindingDetails (see spec/api/bindings_spec.cr)
          # so the two are interchangeable through duck typing.
          tuple = sd.details_tuple
          tuple.keys.to_a.should eq %i[source vhost destination destination_type routing_key arguments properties_key]
          tuple[:source].should eq LavinMQ::MQTT::EXCHANGE
          tuple[:vhost].should eq "/"
          tuple[:destination_type].should eq "queue"
          tuple[:routing_key].should eq "a/b"

          sd.routing_key.should eq "a/b"
          sd.search_match?("a/b").should be_true
          sd.search_match?(/a\/.+/).should be_true

          disconnect(io)
        end
      end
    end
  end
end
