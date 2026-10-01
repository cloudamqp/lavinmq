require "../spec_helper"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers

  describe "MQTT session limit" do
    it "accepts a publish-only CONNECT at max-queues" do
      with_server do |server|
        vhost = server.vhosts["/"]
        vhost.declare_queue("q", false, false)
        vhost.max_queues = 1

        with_client_io(server) do |io|
          connack = connect(io, client_id: "a", clean_session: false).should be_a(MQTT::Protocol::Connack)
          connack.return_code.should eq MQTT::Protocol::Connack::ReturnCode::Accepted
          publish(io, topic: "a/b", qos: 1u8).should be_a(MQTT::Protocol::PubAck)
          disconnect(io)
        end
        vhost.sessions_size.should eq 0
      end
    end

    it "refuses the first SUBSCRIBE past max-queues with Failure" do
      with_server do |server|
        vhost = server.vhosts["/"]
        vhost.declare_queue("q", false, false)
        vhost.max_queues = 1

        with_client_io(server) do |io|
          connect(io, client_id: "a")
          ack = subscribe(io, topic_filters: mk_topic_filters({"a/b", 1u8})).as(MQTT::Protocol::SubAck)
          ack.return_codes.should eq [MQTT::Protocol::SubAck::ReturnCode::Failure]
          pingpong(io).should be_a(MQTT::Protocol::PingResp)
        end
        vhost.sessions_size.should eq 0
        # A refused registration must not leave its client lock behind.
        server.mqtt_server.broker("/").@client_locks.should be_empty
      end
    end

    it "closes a durable QoS 2 publisher whose session cannot become a queue" do
      with_server do |server|
        vhost = server.vhosts["/"]
        vhost.declare_queue("q", false, false)
        with_client_io(server) do |sub_io|
          connect(sub_io, client_id: "sub")
          subscribe(sub_io, topic_filters: mk_topic_filters({"a/b", 0u8}))
          vhost.max_queues = 2

          with_client_io(server) do |io|
            will = MQTT::Protocol::Will.new(topic: "a/b", payload: "will".to_slice, qos: 0u8, retain: false)
            connect(io, client_id: "pub", clean_session: false, will: will)
            # 3.1.1 cannot refuse one PUBLISH, so the connection is closed [MQTT-3.3.5-2]
            publish(io, topic: "a/b", payload: "msg".to_slice, qos: 2u8, packet_id: 1u16, expect_response: false)
            io.should be_closed
          end
          # The Will, as for any close without DISCONNECT [MQTT-3.1.2-8], and not the message
          String.new(read_publish(sub_io).payload).should eq "will"
          sub_io.should be_silent
        end
        vhost.session?("mqtt.pub").should be_nil
      end
    end

    it "lets a persistent client reconnect to its session when the limit is reached" do
      with_server do |server|
        vhost = server.vhosts["/"]

        with_client_io(server) do |io|
          connect(io, client_id: "a", clean_session: false)
          subscribe(io, topic_filters: mk_topic_filters({"a/b", 0}))
          disconnect(io)
        end

        vhost.max_queues = 1

        with_client_io(server) do |io|
          connack = connect(io, client_id: "a", clean_session: false).as(MQTT::Protocol::Connack)
          connack.return_code.should eq MQTT::Protocol::Connack::ReturnCode::Accepted
          connack.session_present?.should be_true
          ack = subscribe(io, topic_filters: mk_topic_filters({"e/f", 0}))
            .should be_a(MQTT::Protocol::SubAck)
          ack.reason_codes.should eq [MQTT::Protocol::SubAck::ReasonCode::GrantedQoS0]
        end
      end
    end

    it "lets a client take over its own connection when the limit is reached" do
      with_server do |server|
        vhost = server.vhosts["/"]
        vhost.max_queues = 1

        with_client_io(server) do |io|
          connect(io, client_id: "a", clean_session: true)
          subscribe(io, topic_filters: mk_topic_filters({"a/b", 0}))

          with_client_io(server) do |io2|
            connack = connect(io2, client_id: "a", clean_session: true).should be_a(MQTT::Protocol::Connack)
            connack.return_code.should eq MQTT::Protocol::Connack::ReturnCode::Accepted
            io.should be_closed
          end
        end
      end
    end

    it "counts AMQP queues and MQTT session queues against the same limit" do
      with_server do |server|
        vhost = server.vhosts["/"]
        with_client_io(server) do |io|
          connect(io, client_id: "a", clean_session: false)
          subscribe(io, topic_filters: mk_topic_filters({"a/b", 0}))
          disconnect(io)
        end
        vhost.max_queues = 1

        vhost.queue_limit_reached?.should be_true
        with_client_io(server) do |io|
          connect(io, client_id: "b")
          ack = subscribe(io, topic_filters: mk_topic_filters({"a/b", 0})).as(MQTT::Protocol::SubAck)
          ack.return_codes.should eq [MQTT::Protocol::SubAck::ReturnCode::Failure]
        end
      end
    end

    it "lets a refused SUBSCRIBE succeed on the same connection once a queue is freed" do
      with_server do |server|
        vhost = server.vhosts["/"]
        vhost.declare_queue("q", false, false)
        vhost.max_queues = 1
        with_client_io(server) do |io|
          connect(io, client_id: "a")
          ack = subscribe(io, topic_filters: mk_topic_filters({"a/b", 0u8})).as(MQTT::Protocol::SubAck)
          ack.return_codes.should eq [MQTT::Protocol::SubAck::ReturnCode::Failure]
          vhost.delete_queue("q")
          ack = subscribe(io, topic_filters: mk_topic_filters({"a/b", 0u8})).as(MQTT::Protocol::SubAck)
          ack.return_codes.should eq [MQTT::Protocol::SubAck::ReturnCode::QoS0]
        end
      end
    end

    it "allows a new session queue once one is freed" do
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
          ack = subscribe(io, topic_filters: mk_topic_filters({"a/b", 0})).as(MQTT::Protocol::SubAck)
          ack.return_codes.should eq [MQTT::Protocol::SubAck::ReturnCode::QoS0]
        end
      end
    end

    it "never deletes a queue, or its files, that only shares a session's name" do
      with_server do |server|
        vhost = server.vhosts["/"]
        with_client_io(server) do |io|
          # Persistent, so its session's directory is the queue's
          connect(io, client_id: "x", clean_session: false)
          # As a definitions import could, while the client has no session queue
          vhost.declare_queue("mqtt.x", true, false)
          # Its session cannot become a queue now
          ack = subscribe(io, topic_filters: mk_topic_filters({"a/b", 0u8})).as(MQTT::Protocol::SubAck)
          ack.return_codes.should eq [MQTT::Protocol::SubAck::ReturnCode::Failure]
          disconnect(io)
        end
        wait_for { server.mqtt_server.broker("/").@clients.empty? }
        vhost.queue?("mqtt.x").should be_truthy
        # Nor its files, which share the session's directory
        Dir.exists?(File.join(vhost.data_dir, Digest::SHA1.hexdigest("mqtt.x"))).should be_true
      end
    end

    it "leaves no session behind when its definition cannot be written" do
      with_server do |server|
        vhost = server.vhosts["/"]
        definitions = vhost.@definitions.should_not be_nil
        with_client_io(server) do |io|
          connect(io, client_id: "a", clean_session: false)
          # As a full disk would
          definitions.@definitions_file.close
          subscribe(io, expect_response: false, topic_filters: mk_topic_filters({"a/b", 0u8}))
          io.should be_closed
        end
        wait_for { server.mqtt_server.broker("/").@clients.empty? }
        vhost.session?("mqtt.a").should be_nil
      end
    end
  end
end
