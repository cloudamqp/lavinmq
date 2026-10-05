require "../spec_helper"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers

  # Publishes `payloads` at QoS 1 from a v5 connection, each with its Message
  # Expiry Interval (nil for none).
  def self.publish_expiring(server, payloads : Array({String, UInt32?}))
    with_client_socket(server) do |socket|
      io = MQTT::Protocol::IO.v5(socket)
      connect(io, version: MQTT::Protocol::Version::V5, client_id: "publisher")
      payloads.each_with_index do |(payload, expiry), i|
        props = MQTT::Protocol::PublishProperties.new
        props.message_expiry_interval = expiry
        publish(io, topic: "a/b", payload: payload.to_slice, qos: 1u8,
          packet_id: (i + 1).to_u16, properties: props)
      end
      disconnect(io)
    end
  end

  # A v5 subscriber to `a/b` at QoS 1 that takes one unacknowledged message at a
  # time, so the rest wait in the session for as long as the spec holds the first.
  def self.connect_v5_subscriber(io, session_expiry : UInt32? = nil, clean_start = true)
    props = MQTT::Protocol::ConnectProperties.new
    props.receive_maximum = 1u16
    props.session_expiry_interval = session_expiry
    connect(io, version: MQTT::Protocol::Version::V5, client_id: "subscriber",
      clean_session: clean_start, properties: props)
  end

  describe "message expiry" do
    it "delivers an unexpired message with its interval intact" do
      with_server do |server|
        with_client_socket(server) do |socket|
          io = MQTT::Protocol::IO.v5(socket)
          connect_v5_subscriber(io)
          subscribe(io, topic_filters: mk_topic_filters({"a/b", 1u8}))
          publish_expiring(server, [{"x", 60u32}])

          read_publish(io).properties.message_expiry_interval.should eq 60u32
          disconnect(io)
        end
      end
    end

    it "deletes a message that expired before delivery started [MQTT-3.3.2-5]", tags: "slow" do
      with_server do |server|
        with_client_socket(server) do |socket|
          io = MQTT::Protocol::IO.v5(socket)
          connect_v5_subscriber(io)
          subscribe(io, topic_filters: mk_topic_filters({"a/b", 1u8}))
          publish_expiring(server, [{"held", nil}, {"expires", 1u32}, {"keeps", nil}])

          held = read_publish(io)
          sleep 1.2.seconds
          puback(io, held.packet_id)
          String.new(read_publish(io).payload).should eq "keeps"
          disconnect(io)
        end
      end
    end

    it "deletes an expired message for a v3.1.1 subscriber too [MQTT-3.3.2-5]", tags: "slow" do
      LavinMQ::Config.instance.max_inflight_messages = 1u16
      with_server do |server|
        with_client_io(server) do |io|
          connect(io, client_id: "subscriber")
          subscribe(io, topic_filters: mk_topic_filters({"a/b", 1u8}))
          publish_expiring(server, [{"held", nil}, {"expires", 1u32}, {"keeps", nil}])

          held = read_publish(io)
          sleep 1.2.seconds
          puback(io, held.packet_id)
          String.new(read_publish(io).payload).should eq "keeps"
          disconnect(io)
        end
      end
    ensure
      LavinMQ::Config.instance.max_inflight_messages = UInt16::MAX
    end

    it "counts the interval down by the time the message waited [MQTT-3.3.2-6]", tags: "slow" do
      with_server do |server|
        with_client_socket(server) do |socket|
          io = MQTT::Protocol::IO.v5(socket)
          connect_v5_subscriber(io)
          subscribe(io, topic_filters: mk_topic_filters({"a/b", 1u8}))
          publish_expiring(server, [{"held", nil}, {"waits", 10u32}])

          held = read_publish(io)
          sleep 1.2.seconds
          puback(io, held.packet_id)
          waited = read_publish(io)
          String.new(waited.payload).should eq "waits"
          interval = waited.properties.message_expiry_interval.should_not be_nil
          interval.should be < 10u32
          interval.should be > 0u32
          disconnect(io)
        end
      end
    end

    it "still redelivers an expired message whose delivery started [MQTT-4.4.0-1]", tags: "slow" do
      with_server do |server|
        with_client_socket(server) do |socket|
          io = MQTT::Protocol::IO.v5(socket)
          connect_v5_subscriber(io, session_expiry: 60u32)
          subscribe(io, topic_filters: mk_topic_filters({"a/b", 1u8}))
          publish_expiring(server, [{"sent", 1u32}])
          read_publish(io).dup?.should be_false
          disconnect(io)
        end

        sleep 1.2.seconds

        with_client_socket(server) do |socket|
          io = MQTT::Protocol::IO.v5(socket)
          connect_v5_subscriber(io, session_expiry: 60u32, clean_start: false)
          resent = read_publish(io)
          String.new(resent.payload).should eq "sent"
          resent.dup?.should be_true
          # Expired by now, so nothing of the interval is left to pass on.
          resent.properties.message_expiry_interval.should eq 0u32
          disconnect(io)
        end
      end
    end
  end
end
