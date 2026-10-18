require "./spec_helper"

module MqttSpecs
  extend MqttHelpers

  # Sessions are only rebuilt as sessions when x-queue-type is present, so every
  # hand-declared session needs it alongside whatever is under test.
  private def self.session_args(expiry : ::AMQ::Protocol::Field = nil)
    args = LavinMQ::AMQP::Table.new
    args["x-queue-type"] = "mqtt"
    args[LavinMQ::MQTT::SESSION_EXPIRY_ARG] = expiry unless expiry.nil?
    args
  end

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

    describe "#session_expiry_interval" do
      it "is read from the queue arguments" do
        with_server do |server|
          vhost = server.vhosts["/"]
          vhost.declare_queue("mqtt.expiry", true, false, session_args(60))
          vhost.session("mqtt.expiry").session_expiry_interval.should eq 60u32
        end
      end

      it "falls back to the declare flag when the argument is unusable" do
        # Reachable from an AMQP client declaring mqtt.<id> by hand, so every
        # branch has to land somewhere sane rather than raise.
        with_server do |server|
          vhost = server.vhosts["/"]
          [-5, Int64::MAX, "3600"].each_with_index do |value, i|
            name = "mqtt.bad#{i}"
            vhost.declare_queue(name, true, false, session_args(value))
            # auto_delete false, so the pre-expiry meaning of the flag: forever.
            vhost.session(name).session_expiry_interval.should eq UInt32::MAX
          end
        end
      end
    end
  end
end
