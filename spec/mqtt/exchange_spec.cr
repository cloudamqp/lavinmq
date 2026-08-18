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
          sd.binding_key.should be_a(LavinMQ::BindingKey)

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
