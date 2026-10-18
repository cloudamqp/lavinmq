require "../spec_helper.cr"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers
  describe "retained messages" do
    it "retained messages are received on subscribe" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io, client_id: "publisher")
          publish(io, topic: "a/b", qos: 0u8, retain: true)
          disconnect(io)
        end

        with_client_io(server) do |io|
          connect(io, client_id: "subscriber")
          subscribe(io, topic_filters: [subtopic("a/b")])
          pub = read_packet(io).as(MQTT::Protocol::Publish)
          pub.topic.should eq("a/b")
          pub.retain?.should be_true
          disconnect(io)
        end
      end
    end

    it "retain flag is 0 if there is an established subscription [MQTT-3.3.1-12]" do
      with_server do |server|
        with_client_io(server) do |sub_io|
          connect(sub_io, client_id: "sub")
          topic_filters = mk_topic_filters({"test", 0})
          subscribe(sub_io, topic_filters: topic_filters)

          with_client_io(server) do |pub_io|
            connect(pub_io, client_id: "pub")
            publish(pub_io, topic: "test", qos: 0u8, retain: true)
          end

          msg = read_packet(sub_io).as(MQTT::Protocol::Publish)
          msg.retain?.should be_false
        end
      end
    end

    it "replays a retained message with its v5 properties [MQTT-3.3.2-17]" do
      with_server do |server|
        with_client_socket(server) do |socket|
          io = MQTT::Protocol::IO.v5(socket)
          connect(io, version: MQTT::Protocol::Version::V5, client_id: "publisher", clean_session: true)
          props = MQTT::Protocol::PublishProperties.new
          props.content_type = "text/plain"
          props.user_properties = [{"k", "v"}]
          publish(io, topic: "a/b", payload: "x".to_slice, qos: 1u8, retain: true, properties: props)
          disconnect(io)
        end

        with_client_socket(server) do |socket|
          io = MQTT::Protocol::IO.v5(socket)
          connect(io, version: MQTT::Protocol::Version::V5, client_id: "subscriber", clean_session: true)
          subscribe(io, topic_filters: [subtopic("a/b", 1u8)])
          pub = read_publish(io)
          pub.retain?.should be_true
          pub.properties.content_type.should eq "text/plain"
          pub.properties.user_properties.should eq [{"k", "v"}]
          disconnect(io)
        end
      end
    end

    it "replays a retained message at the lower of its and the subscription's QoS [MQTT-3.8.4-8]" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io, client_id: "publisher", clean_session: true)
          publish(io, topic: "a/b", qos: 0u8, retain: true)
          disconnect(io)
        end

        with_client_io(server) do |io|
          connect(io, client_id: "subscriber", clean_session: true)
          subscribe(io, topic_filters: [subtopic("a/b", 1u8)])
          read_publish(io).qos.should eq 0u8
          disconnect(io)
        end
      end
    end

    it "replays a retained message with what is left of its Message Expiry Interval [MQTT-3.3.2-6]" do
      with_server do |server|
        with_client_socket(server) do |socket|
          io = MQTT::Protocol::IO.v5(socket)
          connect(io, version: MQTT::Protocol::Version::V5, client_id: "publisher", clean_session: true)
          props = MQTT::Protocol::PublishProperties.new
          props.message_expiry_interval = 60u32
          publish(io, topic: "a/b", payload: "x".to_slice, qos: 1u8, retain: true, properties: props)
          disconnect(io)
        end

        with_client_socket(server) do |socket|
          io = MQTT::Protocol::IO.v5(socket)
          connect(io, version: MQTT::Protocol::Version::V5, client_id: "subscriber", clean_session: true)
          subscribe(io, topic_filters: [subtopic("a/b", 1u8)])
          interval = read_publish(io).properties.message_expiry_interval.should_not be_nil
          interval.should be <= 60u32
          interval.should be > 0u32
          disconnect(io)
        end
      end
    end

    it "replays a retained message stored before the format had a header, then replaces it" do
      # The layout LavinMQ wrote before `.rmsg`: the topic in `index`, the
      # payload alone in `<md5>.msg`.
      dir = File.join(LavinMQ::Config.instance.data_dir, Digest::SHA1.hexdigest("/"), "mqtt_retained_store")
      Dir.mkdir_p(dir)
      File.write(File.join(dir, "index"), "a/b\n")
      legacy = File.join(dir, "#{Digest::MD5.hexdigest("a/b")}.msg")
      File.write(legacy, "old")

      with_server do |server|
        # Read as QoS 1, so a QoS 2 subscription gets QoS 1 and a QoS 0 one
        # gets QoS 0 [MQTT-3.8.4-8].
        {2u8 => 1u8, 0u8 => 0u8}.each do |granted, delivered|
          with_client_io(server) do |io|
            connect(io, client_id: "subscriber", clean_session: true)
            subscribe(io, topic_filters: [subtopic("a/b", granted)])
            pub = read_publish(io)
            String.new(pub.payload).should eq "old"
            pub.retain?.should be_true
            pub.qos.should eq delivered
            disconnect(io)
          end
        end

        with_client_io(server) do |io|
          connect(io, client_id: "publisher", clean_session: true)
          publish(io, topic: "a/b", payload: "new".to_slice, qos: 1u8, retain: true)
          disconnect(io)
        end
        File.exists?(legacy).should be_false
        File.exists?(legacy.rchop(".msg") + ".rmsg").should be_true

        with_client_io(server) do |io|
          connect(io, client_id: "subscriber", clean_session: true)
          subscribe(io, topic_filters: [subtopic("a/b", 1u8)])
          String.new(read_publish(io).payload).should eq "new"
          disconnect(io)
        end
      end
    end

    it "retained messages are redelivered for subscriptions with qos1" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io, client_id: "publisher")
          # QoS 1: a replay goes out at the lower of this and the subscription's
          # QoS [MQTT-3.8.4-8].
          publish(io, topic: "a/b", qos: 1u8, retain: true)
          disconnect(io)
        end

        with_client_io(server) do |io|
          connect(io, client_id: "subscriber")
          subscribe(io, topic_filters: [subtopic("a/b", 1u8)])
          # Dont ack
          pub = read_packet(io).as(MQTT::Protocol::Publish)
          pub.qos.should eq(1u8)
          pub.topic.should eq("a/b")
          pub.retain?.should be_true
          pub.dup?.should be_false
        end

        with_client_io(server) do |io|
          connect(io, client_id: "subscriber")
          pub = read_packet(io).as(MQTT::Protocol::Publish)
          pub.qos.should eq(1u8)
          pub.topic.should eq("a/b")
          pub.retain?.should be_true
          pub.dup?.should be_true
          puback(io, pub.packet_id)
        end
      end
    end
  end
end
