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
  end
end
