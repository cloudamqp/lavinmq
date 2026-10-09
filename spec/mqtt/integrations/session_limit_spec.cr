require "../spec_helper"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers

  describe "MQTT session limit" do
    it "refuses a subscribe that would exceed max-queues" do
      with_server do |server|
        vhost = server.vhosts["/"]
        vhost.max_queues = 1

        with_client_io(server) do |io|
          connect(io, client_id: "a")
          ack = subscribe(io, topic_filters: mk_topic_filters({"a/b", 0}))
            .should be_a(MQTT::Protocol::SubAck)
          ack.return_codes.should eq [MQTT::Protocol::SubAck::ReturnCode::QoS0]
        end

        with_client_io(server) do |io|
          connect(io, client_id: "b")
          ack = subscribe(io, topic_filters: mk_topic_filters({"c/d", 0}, {"e/f", 1}))
            .should be_a(MQTT::Protocol::SubAck)
          # One return code per topic filter [MQTT-3.8.4-5]
          ack.return_codes.should eq [MQTT::Protocol::SubAck::ReturnCode::Failure,
                                      MQTT::Protocol::SubAck::ReturnCode::Failure]
        end

        vhost.sessions_size.should eq 1
        vhost.session?("mqtt.b").should be_nil
      end
    end

    it "allows subscribing on an existing session when the limit is reached" do
      with_server do |server|
        vhost = server.vhosts["/"]

        with_client_io(server) do |io|
          connect(io, client_id: "a", clean_session: false)
          subscribe(io, topic_filters: mk_topic_filters({"a/b", 0}))

          vhost.max_queues = 1
          ack = subscribe(io, topic_filters: mk_topic_filters({"c/d", 1}))
            .should be_a(MQTT::Protocol::SubAck)
          ack.return_codes.should eq [MQTT::Protocol::SubAck::ReturnCode::QoS1]
          disconnect(io)
        end

        # A reconnecting persistent client reuses its session, so it must not be
        # locked out while the vhost is at the limit
        with_client_io(server) do |io|
          connect(io, client_id: "a", clean_session: false)
          ack = subscribe(io, topic_filters: mk_topic_filters({"e/f", 0}))
            .should be_a(MQTT::Protocol::SubAck)
          ack.return_codes.should eq [MQTT::Protocol::SubAck::ReturnCode::QoS0]
        end
      end
    end

    it "counts AMQP queues and MQTT sessions against the same limit" do
      with_server do |server|
        vhost = server.vhosts["/"]
        vhost.declare_queue("q", false, false)
        vhost.max_queues = 1

        with_client_io(server) do |io|
          connect(io, client_id: "a")
          ack = subscribe(io, topic_filters: mk_topic_filters({"a/b", 0}))
            .should be_a(MQTT::Protocol::SubAck)
          ack.return_codes.should eq [MQTT::Protocol::SubAck::ReturnCode::Failure]
        end

        vhost.sessions_size.should eq 0
      end
    end

    it "allows a new session once one is freed" do
      with_server do |server|
        vhost = server.vhosts["/"]
        vhost.max_queues = 1

        with_client_io(server) do |io|
          connect(io, client_id: "a", clean_session: true)
          subscribe(io, topic_filters: mk_topic_filters({"a/b", 0}))
          disconnect(io)
        end
        wait_for { vhost.sessions_size.zero? }

        with_client_io(server) do |io|
          connect(io, client_id: "b", clean_session: true)
          ack = subscribe(io, topic_filters: mk_topic_filters({"a/b", 0}))
            .should be_a(MQTT::Protocol::SubAck)
          ack.return_codes.should eq [MQTT::Protocol::SubAck::ReturnCode::QoS0]
        end
      end
    end

    it "survives unsubscribe and puback from a client that was refused a session" do
      with_server do |server|
        vhost = server.vhosts["/"]
        vhost.declare_queue("q", false, false)
        vhost.max_queues = 1

        with_client_io(server) do |io|
          connect(io, client_id: "a")
          subscribe(io, topic_filters: mk_topic_filters({"a/b", 0}))

          unsubscribe(io, ["a/b"]).should be_a(MQTT::Protocol::UnsubAck)
          io.should_not be_closed

          # Acking something we never delivered is a protocol violation
          puback(io, 1u16)
          io.should be_closed
        end

        with_client_io(server) do |io|
          connect(io, client_id: "b")
          pingpong(io).should be_a(MQTT::Protocol::PingResp)
        end
      end
    end
  end
end
