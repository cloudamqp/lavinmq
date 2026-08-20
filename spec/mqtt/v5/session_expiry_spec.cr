require "../spec_helper"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers

  private def self.v5_connect(socket, expiry : UInt32?, **args)
    io = MQTT::Protocol::IO::V5.new(socket)
    props = MQTT::Protocol::ConnectProperties.new
    props.session_expiry_interval = expiry if expiry
    connect(io, **{version: MQTT::Protocol::Version::V5, properties: props}.merge(args))
    io
  end

  describe "MQTT 5.0 Session Expiry Interval" do
    it "ends the session with the connection when the interval is 0 [MQTT-3.1.2-11]" do
      with_server do |server|
        with_client_socket(server) do |socket|
          io = v5_connect(socket, 0u32, clean_session: false, client_id: "sub")
          subscribe(io, topic_filters: [subtopic("a/b", 1)], packet_id: 1u16)
          disconnect(io)
        end

        with_client_io(server) do |pub|
          connect(pub, client_id: "publisher")
          publish(pub, topic: "a/b", qos: 1u8)
          disconnect(pub)
        end

        wait_for { server.vhosts["/"].session?("mqtt.sub").nil? }

        with_client_socket(server) do |socket|
          io = v5_connect(socket, 0u32, clean_session: false, client_id: "sub")
          read_packet(io).should be_nil
        end
      end
    end

    it "ends the session with the connection when the interval is absent [MQTT-3.1.2-11]" do
      # Absent means 0, so Clean Start = 0 alone is not enough to persist.
      with_server do |server|
        with_client_socket(server) do |socket|
          io = v5_connect(socket, nil, clean_session: false, client_id: "sub")
          subscribe(io, topic_filters: [subtopic("a/b", 1)], packet_id: 1u16)
          disconnect(io)
        end

        wait_for { server.vhosts["/"].session?("mqtt.sub").nil? }
      end
    end

    it "persists the session when Clean Start is 1 and the interval is non-zero" do
      # The v5 idiom for "discard any old session, but persist this one". Under
      # the old clean-session bit this got a transient session.
      with_server do |server|
        with_client_socket(server) do |socket|
          io = v5_connect(socket, 3600u32, clean_session: true, client_id: "sub")
          subscribe(io, topic_filters: [subtopic("a/b", 1)], packet_id: 1u16)
          disconnect(io)
        end

        session = server.vhosts["/"].session?("mqtt.sub").should_not be_nil
        session.durable?.should be_true
        session.session_expiry_interval.should eq 3600u32

        with_client_socket(server) do |socket|
          io = MQTT::Protocol::IO::V5.new(socket)
          props = MQTT::Protocol::ConnectProperties.new
          props.session_expiry_interval = 3600u32
          connack = connect(io, version: MQTT::Protocol::Version::V5,
            clean_session: false, client_id: "sub", properties: props)
            .as(MQTT::Protocol::Connack)
          connack.session_present?.should be_true
        end
      end
    end

    it "delivers messages published while a non-zero-interval session was offline" do
      with_server do |server|
        with_client_socket(server) do |socket|
          io = v5_connect(socket, 3600u32, clean_session: false, client_id: "sub")
          subscribe(io, topic_filters: [subtopic("a/b", 1)], packet_id: 1u16)
          disconnect(io)
        end

        with_client_io(server) do |pub|
          connect(pub, client_id: "publisher")
          publish(pub, topic: "a/b", payload: "stored".to_slice, qos: 1u8)
          disconnect(pub)
        end

        with_client_socket(server) do |socket|
          io = v5_connect(socket, 3600u32, clean_session: false, client_id: "sub")
          pub = MQTT::Protocol::Packet.from_io(io).as(MQTT::Protocol::Publish)
          pub.payload.should eq "stored".to_slice
          puback(io, pub.packet_id)
        end
      end
    end

    it "adopts the interval named by a reconnecting client" do
      with_server do |server|
        with_client_socket(server) do |socket|
          io = v5_connect(socket, 3600u32, clean_session: false, client_id: "sub")
          subscribe(io, topic_filters: [subtopic("a/b", 1)], packet_id: 1u16)
          disconnect(io)
        end

        with_client_socket(server) do |socket|
          io = v5_connect(socket, 60u32, clean_session: false, client_id: "sub")
          server.vhosts["/"].session("mqtt.sub").session_expiry_interval.should eq 60u32
          disconnect(io)
        end
      end
    end

    it "deletes the session once the interval elapses", tags: "slow" do
      with_server do |server|
        with_client_socket(server) do |socket|
          io = v5_connect(socket, 1u32, clean_session: false, client_id: "sub")
          subscribe(io, topic_filters: [subtopic("a/b", 1)], packet_id: 1u16)
          disconnect(io)
        end

        server.vhosts["/"].session?("mqtt.sub").should_not be_nil
        wait_for { server.vhosts["/"].session?("mqtt.sub").nil? }
      end
    end

    it "cancels the expiry when the client reconnects", tags: "slow" do
      with_server do |server|
        with_client_socket(server) do |socket|
          io = v5_connect(socket, 1u32, clean_session: false, client_id: "sub")
          subscribe(io, topic_filters: [subtopic("a/b", 1)], packet_id: 1u16)
          disconnect(io)
        end

        with_client_socket(server) do |socket|
          v5_connect(socket, 1u32, clean_session: false, client_id: "sub")
          # Past the interval, but attached the whole time, so the timer the
          # previous disconnect started must have been cancelled.
          sleep 1.5.seconds
          server.vhosts["/"].session?("mqtt.sub").should_not be_nil
        end
      end
    end

    it "restores the interval and restarts the clock after a restart" do
      # The interval persists via the queue arguments; the deadline deliberately
      # does not, so a restored session gets its full interval again from boot.
      with_server(clean_dir: false) do |server|
        with_client_socket(server) do |socket|
          io = v5_connect(socket, 3600u32, clean_session: false, client_id: "sub")
          subscribe(io, topic_filters: [subtopic("a/b", 1)], packet_id: 1u16)
          disconnect(io)
        end
      end

      with_server do |server|
        session = server.vhosts["/"].session?("mqtt.sub").should_not be_nil
        session.session_expiry_interval.should eq 3600u32
        session.durable?.should be_true
      end
    end

    it "adopts a new interval from DISCONNECT [MQTT-3.14.2.2.2]" do
      with_server do |server|
        with_client_socket(server) do |socket|
          io = v5_connect(socket, 3600u32, clean_session: false, client_id: "sub")
          subscribe(io, topic_filters: [subtopic("a/b", 1)], packet_id: 1u16)

          # Narrowing to 0 on the way out ends the session with this connection,
          # which is how a client says it is done with it.
          props = MQTT::Protocol::DisconnectProperties.new
          props.session_expiry_interval = 0u32
          MQTT::Protocol::Disconnect.new(
            MQTT::Protocol::Disconnect::ReasonCode::NormalDisconnection, props).to_io(io)
          io.flush
        end

        wait_for { server.vhosts["/"].session?("mqtt.sub").nil? }
      end
    end

    it "keeps the CONNECT interval when DISCONNECT omits it [MQTT-3.14.2.2.2]" do
      # Absent on DISCONNECT means "keep the CONNECT value", not 0.
      with_server do |server|
        with_client_socket(server) do |socket|
          io = v5_connect(socket, 3600u32, clean_session: false, client_id: "sub")
          subscribe(io, topic_filters: [subtopic("a/b", 1)], packet_id: 1u16)
          disconnect(io)
        end

        session = server.vhosts["/"].session?("mqtt.sub").should_not be_nil
        session.session_expiry_interval.should eq 3600u32
      end
    end

    it "answers 0x82 to a non-zero DISCONNECT interval after a zero CONNECT [MQTT-3.14.2]" do
      with_server do |server|
        with_client_socket(server) do |socket|
          io = v5_connect(socket, 0u32, clean_session: false, client_id: "sub")

          props = MQTT::Protocol::DisconnectProperties.new
          props.session_expiry_interval = 60u32
          MQTT::Protocol::Disconnect.new(
            MQTT::Protocol::Disconnect::ReasonCode::NormalDisconnection, props).to_io(io)
          io.flush

          pkt = MQTT::Protocol::Packet.from_io(io)
          pkt.should be_a(MQTT::Protocol::Disconnect)
          pkt.as(MQTT::Protocol::Disconnect).reason_code
            .should eq(MQTT::Protocol::Disconnect::ReasonCode::ProtocolError)
        end
      end
    end

    it "publishes the will when a DISCONNECT is rejected as a protocol error" do
      # An invalid DISCONNECT is not a graceful one, so the will fires - and it
      # must fire exactly once. Reason 0x04 is the case that matters: it publishes
      # the will on its own, so handling the expiry after it would publish twice.
      with_server do |server|
        with_client_io(server) do |watcher|
          connect(watcher, client_id: "watcher")
          subscribe(watcher, topic_filters: [subtopic("will/t", 0u8)], packet_id: 9u16)

          with_client_socket(server) do |socket|
            io = v5_connect(socket, 0u32, clean_session: false, client_id: "willer",
              will: MQTT::Protocol::Will.new(topic: "will/t", payload: "dead".to_slice,
                qos: 0u8, retain: false))
            props = MQTT::Protocol::DisconnectProperties.new
            props.session_expiry_interval = 60u32
            MQTT::Protocol::Disconnect.new(
              MQTT::Protocol::Disconnect::ReasonCode::DisconnectWithWillMessage, props).to_io(io)
            io.flush
          end

          pub = read_packet(watcher).as(MQTT::Protocol::Publish)
          pub.payload.should eq "dead".to_slice
          read_packet(watcher).should be_nil
        end
      end
    end

    it "keeps a v3 non-clean session forever" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io, clean_session: false, client_id: "v3sub")
          subscribe(io, topic_filters: [subtopic("a/b", 1)], packet_id: 1u16)
          disconnect(io)
        end

        session = server.vhosts["/"].session?("mqtt.v3sub").should_not be_nil
        session.session_expiry_interval.should eq UInt32::MAX
        session.durable?.should be_true
      end
    end
  end
end
