require "../spec_helper"
require "./protobuf_validator_spec"

module SparkplugSpecs
  extend MqttHelpers
  extend MqttMatchers

  describe "Sparkplug 3.0 integration" do
    it "auto-retains NBIRTH messages even without retain flag" do
      with_server do |server|
        vhost = server.vhosts["/"]
        vhost.sparkplug_aware = true

        # Create valid NBIRTH payload
        nbirth_payload = ProtobufBuilder.build_payload(
          timestamp: 1234567890_u64,
          metrics: ["temperature"],
          seq: 1_u64,
          bdseq: 0_u64
        )

        with_client_io(server) do |io|
          connect(io, client_id: "publisher")

          # Publish NBIRTH without retain flag
          publish(io, topic: "spBv3.0/group1/NBIRTH/node1",
            payload: nbirth_payload, retain: false, qos: 0u8, expect_response: false)

          disconnect(io)
        end

        # Verify it was retained by subscribing
        with_client_io(server) do |io|
          connect(io, client_id: "subscriber")

          # Subscribe to NBIRTH topic
          topic_filters = mk_topic_filters({"spBv3.0/group1/NBIRTH/node1", 0})
          subscribe(io, topic_filters: topic_filters)

          # Should receive retained message
          pub = read_packet(io)
          pub.should be_a(MQTT::Protocol::Publish)
          pub = pub.as(MQTT::Protocol::Publish)
          pub.payload.should eq(nbirth_payload)
          pub.retain?.should be_true

          disconnect(io)
        end
      end
    end

    it "auto-retains DBIRTH messages even without retain flag" do
      with_server do |server|
        vhost = server.vhosts["/"]
        vhost.sparkplug_aware = true

        # Create valid DBIRTH payload
        dbirth_payload = ProtobufBuilder.build_payload(
          timestamp: 1234567890_u64,
          metrics: ["sensor1"],
          seq: 1_u64,
          bdseq: 0_u64
        )

        with_client_io(server) do |io|
          connect(io, client_id: "publisher")

          # Publish DBIRTH without retain flag
          publish(io, topic: "spBv3.0/group1/DBIRTH/node1/device1",
            payload: dbirth_payload, retain: false, qos: 0u8, expect_response: false)

          disconnect(io)
        end

        # Verify it was retained
        with_client_io(server) do |io|
          connect(io, client_id: "subscriber")

          topic_filters = mk_topic_filters({"spBv3.0/group1/DBIRTH/node1/device1", 0})
          subscribe(io, topic_filters: topic_filters)

          pub = read_packet(io)
          pub.should be_a(MQTT::Protocol::Publish)
          pub = pub.as(MQTT::Protocol::Publish)
          pub.payload.should eq(dbirth_payload)
          pub.retain?.should be_true

          disconnect(io)
        end
      end
    end

    it "allows certificate access via $sparkplug/certificates/#" do
      with_server do |server|
        vhost = server.vhosts["/"]
        vhost.sparkplug_aware = true

        # Create valid NBIRTH payloads
        node1_payload = ProtobufBuilder.build_payload(
          timestamp: 1234567890_u64,
          metrics: ["node1_temp"],
          seq: 1_u64,
          bdseq: 0_u64
        )
        node2_payload = ProtobufBuilder.build_payload(
          timestamp: 1234567891_u64,
          metrics: ["node2_temp"],
          seq: 1_u64,
          bdseq: 0_u64
        )

        with_client_io(server) do |io|
          connect(io, client_id: "publisher")

          # Publish NBIRTH messages
          publish(io, topic: "spBv3.0/group1/NBIRTH/node1",
            payload: node1_payload, retain: true, qos: 0u8, expect_response: false)
          publish(io, topic: "spBv3.0/group1/NBIRTH/node2",
            payload: node2_payload, retain: true, qos: 0u8, expect_response: false)

          disconnect(io)
        end

        # Subscribe to certificate wildcard
        with_client_io(server) do |io|
          connect(io, client_id: "subscriber")

          # Subscribe to certificate access topic
          topic_filters = mk_topic_filters({"$sparkplug/certificates/#", 0})
          subscribe(io, topic_filters: topic_filters)

          # Should receive both NBIRTH certificates
          messages = [] of Bytes
          2.times do
            if pub = read_packet(io)
              pub.should be_a(MQTT::Protocol::Publish)
              pub = pub.as(MQTT::Protocol::Publish)
              messages << pub.payload
            end
          end

          messages.should contain(node1_payload)
          messages.should contain(node2_payload)

          disconnect(io)
        end
      end
    end

    it "rejects invalid Sparkplug topics when sparkplug_aware enabled" do
      with_server do |server|
        vhost = server.vhosts["/"]
        vhost.sparkplug_aware = true

        with_client_io(server) do |io|
          connect(io, client_id: "publisher")

          # Try to publish invalid topic (wrong message type)
          publish(io, topic: "spBv3.0/group1/INVALID/node1",
            payload: "test".to_slice, qos: 0u8, expect_response: false)

          # Connection should be closed due to validation error
          sleep 0.1.seconds
          expect_raises(IO::EOFError) do
            read_packet(io)
          end
        end
      end
    end

    it "rejects malformed Sparkplug topics with too few segments" do
      with_server do |server|
        vhost = server.vhosts["/"]
        vhost.sparkplug_aware = true

        with_client_io(server) do |io|
          connect(io, client_id: "publisher")

          # Topic in the reserved namespace but missing message_type/edge_node_id
          publish(io, topic: "spBv3.0/group1",
            payload: "test".to_slice, qos: 0u8, expect_response: false)

          # Connection should be closed due to validation error
          sleep 0.1.seconds
          expect_raises(IO::EOFError) do
            read_packet(io)
          end
        end
      end
    end

    it "rejects DBIRTH without device_id" do
      with_server do |server|
        vhost = server.vhosts["/"]
        vhost.sparkplug_aware = true

        with_client_io(server) do |io|
          connect(io, client_id: "publisher")

          # Try to publish DBIRTH without device_id (invalid)
          publish(io, topic: "spBv3.0/group1/DBIRTH/node1",
            payload: "test".to_slice, qos: 0u8, expect_response: false)

          # Connection should be closed
          sleep 0.1.seconds
          expect_raises(IO::EOFError) do
            read_packet(io)
          end
        end
      end
    end

    it "rejects publishing to certificate topics" do
      with_server do |server|
        vhost = server.vhosts["/"]
        vhost.sparkplug_aware = true

        with_client_io(server) do |io|
          connect(io, client_id: "publisher")

          # Try to publish to certificate topic (not allowed)
          publish(io, topic: "$sparkplug/certificates/group1/NBIRTH/node1",
            payload: "test".to_slice, qos: 0u8, expect_response: false)

          # Connection should be closed
          sleep 0.1.seconds
          expect_raises(IO::EOFError) do
            read_packet(io)
          end
        end
      end
    end

    it "accepts BIRTH and DEATH messages" do
      with_server do |server|
        vhost = server.vhosts["/"]
        vhost.sparkplug_aware = true

        # Create valid NBIRTH and NDEATH payloads
        nbirth_payload = ProtobufBuilder.build_payload(
          timestamp: 1234567890_u64,
          metrics: ["temp"],
          seq: 1_u64,
          bdseq: 0_u64
        )
        ndeath_payload = ProtobufBuilder.build_payload(
          timestamp: 1234567891_u64,
          bdseq: 0_u64
        )

        with_client_io(server) do |io|
          connect(io, client_id: "edge_node")

          # Publish valid NBIRTH and NDEATH messages and verify they're accepted
          publish(io, topic: "spBv3.0/group1/NBIRTH/node1",
            payload: nbirth_payload, qos: 0u8, expect_response: false)

          publish(io, topic: "spBv3.0/group1/NDEATH/node1",
            payload: ndeath_payload, qos: 0u8, expect_response: false)

          # Verify connection is still open (messages were accepted)
          ping(io)
          read_packet(io).should be_a(MQTT::Protocol::PingResp)

          disconnect(io)
        end
      end
    end

    it "doesn't validate when sparkplug_aware is false (backward compat)" do
      with_server do |server|
        vhost = server.vhosts["/"]
        vhost.sparkplug_aware = false # Explicitly disabled

        with_client_io(server) do |io|
          connect(io, client_id: "publisher")

          # Publish invalid Sparkplug topic (should be accepted)
          publish(io, topic: "spBv3.0/group1/INVALID/node1",
            payload: "test".to_slice, qos: 0u8, expect_response: false)

          # Publish to certificate topic (should be accepted)
          publish(io, topic: "$sparkplug/certificates/test",
            payload: "test".to_slice, qos: 0u8, expect_response: false)

          # Connection should remain open
          ping(io)
          read_packet(io).should be_a(MQTT::Protocol::PingResp)

          disconnect(io)
        end
      end
    end

    it "allows non-Sparkplug topics when sparkplug_aware enabled" do
      with_server do |server|
        vhost = server.vhosts["/"]
        vhost.sparkplug_aware = true

        with_client_io(server) do |io|
          connect(io, client_id: "publisher")

          # Publish regular MQTT topics (should work fine)
          publish(io, topic: "sensor/temperature",
            payload: "25.5".to_slice, qos: 0u8, expect_response: false)
          publish(io, topic: "home/lights/living_room",
            payload: "on".to_slice, qos: 0u8, expect_response: false)

          # Connection should remain open
          ping(io)
          read_packet(io).should be_a(MQTT::Protocol::PingResp)

          disconnect(io)
        end
      end
    end

    it "accepts valid Sparkplug message types" do
      with_server do |server|
        vhost = server.vhosts["/"]
        vhost.sparkplug_aware = true

        # Create valid payloads for different message types
        nbirth_payload = ProtobufBuilder.build_payload(timestamp: 1_u64, metrics: ["m1"], seq: 1_u64, bdseq: 0_u64)
        ndeath_payload = ProtobufBuilder.build_payload(bdseq: 0_u64)
        ndata_payload = ProtobufBuilder.build_payload(seq: 1_u64)
        ncmd_payload = Bytes.empty  # CMD messages have relaxed validation
        state_payload = Bytes.empty # STATE messages have relaxed validation
        dbirth_payload = ProtobufBuilder.build_payload(timestamp: 1_u64, metrics: ["m1"], seq: 1_u64, bdseq: 0_u64)
        ddeath_payload = ProtobufBuilder.build_payload(bdseq: 0_u64)
        ddata_payload = ProtobufBuilder.build_payload(seq: 1_u64)
        dcmd_payload = Bytes.empty

        with_client_io(server) do |io|
          connect(io, client_id: "publisher")

          # Test all valid message types
          publish(io, topic: "spBv3.0/group1/NBIRTH/node1",
            payload: nbirth_payload, qos: 0u8, expect_response: false)
          publish(io, topic: "spBv3.0/group1/NDEATH/node1",
            payload: ndeath_payload, qos: 0u8, expect_response: false)
          publish(io, topic: "spBv3.0/group1/NDATA/node1",
            payload: ndata_payload, qos: 0u8, expect_response: false)
          publish(io, topic: "spBv3.0/group1/NCMD/node1",
            payload: ncmd_payload, qos: 0u8, expect_response: false)
          publish(io, topic: "spBv3.0/group1/STATE/host1",
            payload: state_payload, qos: 0u8, expect_response: false)
          publish(io, topic: "spBv3.0/group1/DBIRTH/node1/device1",
            payload: dbirth_payload, qos: 0u8, expect_response: false)
          publish(io, topic: "spBv3.0/group1/DDEATH/node1/device1",
            payload: ddeath_payload, qos: 0u8, expect_response: false)
          publish(io, topic: "spBv3.0/group1/DDATA/node1/device1",
            payload: ddata_payload, qos: 0u8, expect_response: false)
          publish(io, topic: "spBv3.0/group1/DCMD/node1/device1",
            payload: dcmd_payload, qos: 0u8, expect_response: false)

          # Connection should remain open
          ping(io)
          read_packet(io).should be_a(MQTT::Protocol::PingResp)

          disconnect(io)
        end
      end
    end

    it "expands group-specific certificate subscription" do
      with_server do |server|
        vhost = server.vhosts["/"]
        vhost.sparkplug_aware = true

        # Create valid NBIRTH payloads for different groups
        group1_payload = ProtobufBuilder.build_payload(
          timestamp: 1234567890_u64,
          metrics: ["group1_metric"],
          seq: 1_u64,
          bdseq: 0_u64
        )
        group2_payload = ProtobufBuilder.build_payload(
          timestamp: 1234567891_u64,
          metrics: ["group2_metric"],
          seq: 1_u64,
          bdseq: 0_u64
        )

        with_client_io(server) do |io|
          connect(io, client_id: "publisher")

          # Publish NBIRTH for different groups
          publish(io, topic: "spBv3.0/group1/NBIRTH/node1",
            payload: group1_payload, retain: true, qos: 0u8, expect_response: false)
          publish(io, topic: "spBv3.0/group2/NBIRTH/node1",
            payload: group2_payload, retain: true, qos: 0u8, expect_response: false)

          disconnect(io)
        end

        # Subscribe to group1-specific certificates only
        with_client_io(server) do |io|
          connect(io, client_id: "subscriber")

          topic_filters = mk_topic_filters({"$sparkplug/certificates/group1/#", 0})
          subscribe(io, topic_filters: topic_filters)

          # Should only receive group1 NBIRTH
          pub = read_packet(io)
          pub.should be_a(MQTT::Protocol::Publish)
          pub = pub.as(MQTT::Protocol::Publish)
          pub.payload.should eq(group1_payload)

          # Should not receive group2 (timeout indicates no message)
          read_packet(io).should be_nil

          disconnect(io)
        end
      end
    end

    it "returns a failure code for a certificate filter that expands to nothing" do
      with_server do |server|
        vhost = server.vhosts["/"]
        vhost.sparkplug_aware = true

        with_client_io(server) do |io|
          connect(io, client_id: "subscriber")

          # "$sparkplug/certificates/group1" is incomplete and maps to no real topic
          topic_filters = mk_topic_filters({"$sparkplug/certificates/group1", 0})
          suback = subscribe(io, topic_filters: topic_filters)

          suback.should be_a(MQTT::Protocol::SubAck)
          suback = suback.as(MQTT::Protocol::SubAck)
          # Exactly one return code per requested filter (MQTT 3.1.1 §3.9)
          suback.return_codes.size.should eq(1)
          suback.return_codes.first.should eq(MQTT::Protocol::SubAck::ReturnCode::Failure)

          disconnect(io)
        end
      end
    end

    it "returns one return code for a certificate wildcard that expands to many topics" do
      with_server do |server|
        vhost = server.vhosts["/"]
        vhost.sparkplug_aware = true

        with_client_io(server) do |io|
          connect(io, client_id: "subscriber")

          # Expands to 2 actual topics, but the client subscribed to 1 filter
          topic_filters = mk_topic_filters({"$sparkplug/certificates/#", 0})
          suback = subscribe(io, topic_filters: topic_filters)

          suback = suback.as(MQTT::Protocol::SubAck)
          suback.return_codes.size.should eq(1)
          suback.return_codes.first.should eq(MQTT::Protocol::SubAck::ReturnCode::QoS0)

          disconnect(io)
        end
      end
    end

    it "persists sparkplug_aware config across restarts" do
      path = LavinMQ::Config.instance.data_dir

      # Start server and enable sparkplug_aware
      with_server(clean_dir: false) do |server|
        vhost = server.vhosts["/"]
        vhost.sparkplug_aware = true
        vhost.sparkplug_aware?.should be_true
      end

      # Restart server and check if setting persists
      with_server(clean_dir: false) do |server|
        vhost = server.vhosts["/"]
        vhost.sparkplug_aware?.should be_true
      end
    ensure
      FileUtils.rm_rf(path) if path
    end
  end
end
