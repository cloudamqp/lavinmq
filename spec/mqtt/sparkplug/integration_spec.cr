require "../spec_helper"

module SparkplugSpecs
  extend MqttHelpers
  extend MqttMatchers

  describe "Sparkplug 3.0 integration" do
    it "auto-retains NBIRTH messages even without retain flag" do
      with_server do |server|
        vhost = server.vhosts["/"]
        vhost.sparkplug_aware = true

        with_client_io(server) do |io|
          connect(io, client_id: "publisher")

          # Publish NBIRTH without retain flag
          publish(io, topic: "spBv3.0/group1/NBIRTH/node1",
            payload: "birth_payload".to_slice, retain: false, qos: 0u8, expect_response: false)

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
          String.new(pub.payload).should eq("birth_payload")
          pub.retain?.should be_true

          disconnect(io)
        end
      end
    end

    it "auto-retains DBIRTH messages even without retain flag" do
      with_server do |server|
        vhost = server.vhosts["/"]
        vhost.sparkplug_aware = true

        with_client_io(server) do |io|
          connect(io, client_id: "publisher")

          # Publish DBIRTH without retain flag
          publish(io, topic: "spBv3.0/group1/DBIRTH/node1/device1",
            payload: "device_birth".to_slice, retain: false, qos: 0u8, expect_response: false)

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
          String.new(pub.payload).should eq("device_birth")
          pub.retain?.should be_true

          disconnect(io)
        end
      end
    end

    it "allows certificate access via $sparkplug/certificates/#" do
      with_server do |server|
        vhost = server.vhosts["/"]
        vhost.sparkplug_aware = true

        with_client_io(server) do |io|
          connect(io, client_id: "publisher")

          # Publish NBIRTH messages
          publish(io, topic: "spBv3.0/group1/NBIRTH/node1",
            payload: "node1_birth".to_slice, retain: true, qos: 0u8, expect_response: false)
          publish(io, topic: "spBv3.0/group1/NBIRTH/node2",
            payload: "node2_birth".to_slice, retain: true, qos: 0u8, expect_response: false)

          disconnect(io)
        end

        # Subscribe to certificate wildcard
        with_client_io(server) do |io|
          connect(io, client_id: "subscriber")

          # Subscribe to certificate access topic
          topic_filters = mk_topic_filters({"$sparkplug/certificates/#", 0})
          subscribe(io, topic_filters: topic_filters)

          # Should receive both NBIRTH certificates
          messages = [] of String
          2.times do
            if pub = read_packet(io)
              pub.should be_a(MQTT::Protocol::Publish)
              pub = pub.as(MQTT::Protocol::Publish)
              messages << String.new(pub.payload)
            end
          end

          messages.should contain("node1_birth")
          messages.should contain("node2_birth")

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

    it "tracks edge node state from BIRTH/DEATH messages" do
      with_server do |server|
        vhost = server.vhosts["/"]
        vhost.sparkplug_aware = true

        with_client_io(server) do |io|
          connect(io, client_id: "edge_node")

          # Publish valid NBIRTH and NDEATH messages
          # The state tracking is tested in unit tests, here we just verify they're accepted
          publish(io, topic: "spBv3.0/group1/NBIRTH/node1",
            payload: "birth".to_slice, qos: 0u8, expect_response: false)

          publish(io, topic: "spBv3.0/group1/NDEATH/node1",
            payload: "death".to_slice, qos: 0u8, expect_response: false)

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

        with_client_io(server) do |io|
          connect(io, client_id: "publisher")

          # Test all valid message types
          publish(io, topic: "spBv3.0/group1/NBIRTH/node1",
            payload: "".to_slice, qos: 0u8, expect_response: false)
          publish(io, topic: "spBv3.0/group1/NDEATH/node1",
            payload: "".to_slice, qos: 0u8, expect_response: false)
          publish(io, topic: "spBv3.0/group1/NDATA/node1",
            payload: "".to_slice, qos: 0u8, expect_response: false)
          publish(io, topic: "spBv3.0/group1/NCMD/node1",
            payload: "".to_slice, qos: 0u8, expect_response: false)
          publish(io, topic: "spBv3.0/group1/STATE/host1",
            payload: "".to_slice, qos: 0u8, expect_response: false)
          publish(io, topic: "spBv3.0/group1/DBIRTH/node1/device1",
            payload: "".to_slice, qos: 0u8, expect_response: false)
          publish(io, topic: "spBv3.0/group1/DDEATH/node1/device1",
            payload: "".to_slice, qos: 0u8, expect_response: false)
          publish(io, topic: "spBv3.0/group1/DDATA/node1/device1",
            payload: "".to_slice, qos: 0u8, expect_response: false)
          publish(io, topic: "spBv3.0/group1/DCMD/node1/device1",
            payload: "".to_slice, qos: 0u8, expect_response: false)

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

        with_client_io(server) do |io|
          connect(io, client_id: "publisher")

          # Publish NBIRTH for different groups
          publish(io, topic: "spBv3.0/group1/NBIRTH/node1",
            payload: "group1_node1".to_slice, retain: true, qos: 0u8, expect_response: false)
          publish(io, topic: "spBv3.0/group2/NBIRTH/node1",
            payload: "group2_node1".to_slice, retain: true, qos: 0u8, expect_response: false)

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
          String.new(pub.payload).should eq("group1_node1")

          # Should not receive group2 (timeout indicates no message)
          read_packet(io).should be_nil

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
