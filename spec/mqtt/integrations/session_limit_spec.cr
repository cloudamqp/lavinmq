require "../spec_helper"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers

  describe "MQTT session limit" do
    it "refuses a connect that would exceed max-queues" do
      with_server do |server|
        vhost = server.vhosts["/"]
        vhost.max_queues = 1

        with_client_io(server) do |io|
          connack = connect(io, client_id: "a").should be_a(MQTT::Protocol::Connack)
          connack.reason_code.should eq MQTT::Protocol::Connack::ReasonCode::Success

          with_client_io(server) do |io2|
            connack = connect(io2, client_id: "b").should be_a(MQTT::Protocol::Connack)
            connack.reason_code.should eq MQTT::Protocol::Connack::ReasonCode::ServerUnavailable
            io2.should be_closed
          end
        end

        vhost.session?("mqtt.b").should be_nil
        # A refused CONNECT must not leave its client lock behind.
        server.mqtt_server.broker("/").@client_locks.should be_empty
      end
    end

    it "counts a session without subscriptions" do
      with_server do |server|
        vhost = server.vhosts["/"]

        with_client_io(server) do |io|
          connect(io, client_id: "a", clean_session: false)
          disconnect(io)
        end

        vhost.session?("mqtt.a").should_not be_nil
        vhost.max_queues = 1

        with_client_io(server) do |io|
          connack = connect(io, client_id: "b").should be_a(MQTT::Protocol::Connack)
          connack.reason_code.should eq MQTT::Protocol::Connack::ReasonCode::ServerUnavailable
        end
      end
    end

    it "lets a persistent client reconnect to its session when the limit is reached" do
      with_server do |server|
        vhost = server.vhosts["/"]

        with_client_io(server) do |io|
          connect(io, client_id: "a", clean_session: false)
          disconnect(io)
        end

        vhost.max_queues = 1

        with_client_io(server) do |io|
          connack = connect(io, client_id: "a", clean_session: false).should be_a(MQTT::Protocol::Connack)
          connack.reason_code.should eq MQTT::Protocol::Connack::ReasonCode::Success
          ack = subscribe(io, topic_filters: mk_topic_filters({"e/f", 0}))
            .should be_a(MQTT::Protocol::SubAck)
          ack.reason_codes.should eq [MQTT::Protocol::SubAck::ReasonCode::GrantedQos0]
        end
      end
    end

    it "lets a client take over its own connection when the limit is reached" do
      with_server do |server|
        vhost = server.vhosts["/"]
        vhost.max_queues = 1

        with_client_io(server) do |io|
          connect(io, client_id: "a", clean_session: true)

          with_client_io(server) do |io2|
            connack = connect(io2, client_id: "a", clean_session: true).should be_a(MQTT::Protocol::Connack)
            connack.reason_code.should eq MQTT::Protocol::Connack::ReasonCode::Success
            io.should be_closed
          end
        end
      end
    end

    it "counts AMQP queues and MQTT sessions against the same limit" do
      with_server do |server|
        vhost = server.vhosts["/"]
        vhost.declare_queue("q", false, false)
        vhost.max_queues = 1

        with_client_io(server) do |io|
          connack = connect(io, client_id: "a").should be_a(MQTT::Protocol::Connack)
          connack.reason_code.should eq MQTT::Protocol::Connack::ReasonCode::ServerUnavailable
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
          disconnect(io)
        end
        wait_for { vhost.sessions_size.zero? }

        with_client_io(server) do |io|
          connack = connect(io, client_id: "b", clean_session: true).should be_a(MQTT::Protocol::Connack)
          connack.reason_code.should eq MQTT::Protocol::Connack::ReasonCode::Success
        end
      end
    end
  end
end
