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
