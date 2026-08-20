require "./spec_helper"

module MqttSpecs
  extend MqttHelpers

  describe LavinMQ::MQTT::Session do
    describe "#arguments" do
      it "reports the arguments the session was declared with" do
        with_server do |server|
          vhost = server.vhosts["/"]
          args = LavinMQ::AMQP::Table.new({"x-queue-type" => "mqtt", "x-spec" => "kept"})
          vhost.declare_queue("mqtt.args", true, false, args)
          vhost.session("mqtt.args").arguments["x-spec"]?.should eq "kept"
        end
      end

      it "carries x-queue-type so a persisted session is rebuilt as a session" do
        # definitions_store compaction writes `s.arguments` into the replayed
        # Queue::Declare frame, and QueueFactory decides Session vs AMQP::Queue
        # by looking for this key in it.
        with_server do |server|
          with_client_io(server) do |io|
            connect(io, clean_session: false)
            subscribe(io, topic_filters: mk_topic_filters({"a/b", 0u8}))
            server.vhosts["/"].session("mqtt.client_id")
              .arguments["x-queue-type"]?.should eq "mqtt"
            disconnect(io)
          end
        end
      end
    end
  end
end
