require "./spec_helper"
require "./mqtt/spec_helper"
require "../src/lavinmq/config"

describe LavinMQ::VHost do
  describe "Stats" do
    it "should update queue data" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        s.update_stats_rates

        initial_declared_queues = vhost.queue_declared_count
        initial_deleted_queues = vhost.queue_deleted_count
        s.vhosts["/"].declare_queue("this_queue_should_not_exist", false, false)
        s.update_stats_rates

        vhost.queue_declared_count.should eq(initial_declared_queues + 1)
        vhost.queue_deleted_count.should eq initial_deleted_queues
        s.vhosts["/"].delete_queue("this_queue_should_not_exist")
        s.update_stats_rates

        vhost.queue_declared_count.should eq(initial_declared_queues + 1)
        vhost.queue_deleted_count.should eq(initial_deleted_queues + 1)
      end
    end

    it "should not delete stats when connection is closed" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        wait_for { s.connections.sum(&.channels_size).zero? }

        s.update_stats_rates
        initial_channels_created = vhost.channel_created_count
        initial_channels_closed = vhost.channel_closed_count

        with_channel(s) do |ch|
          s.update_stats_rates

          vhost.channel_created_count.should eq(initial_channels_created + 1)
          vhost.channel_closed_count.should eq(initial_channels_closed)
          ch.close
        end

        s.update_stats_rates
        vhost.channel_created_count.should eq(initial_channels_created + 1)
        vhost.channel_closed_count.should eq(initial_channels_closed + 1)
      end
    end

    it "should count connections open / closed once" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        initial_connections_created = vhost.connection_created_count
        initial_connections_closed = vhost.connection_closed_count

        with_channel(s) do
          s.update_stats_rates
          vhost.connection_created_count.should eq(initial_connections_created + 1)
          vhost.connection_closed_count.should eq(initial_connections_closed)
        end

        wait_for { vhost.@connections.empty? }

        s.update_stats_rates
        vhost.connection_created_count.should eq(initial_connections_created + 1)
        vhost.connection_closed_count.should eq(initial_connections_closed + 1)
      end
    end

    it "should count get_no_ack" do
      with_amqp_server do |s|
        with_channel(s) do |c|
          vhost = s.vhosts["/"]
          s.update_stats_rates
          vhost.get_no_ack_count.should eq 0
          q = c.queue("q1")
          q.publish "not acked message"

          q.get(true)
          s.update_stats_rates
          vhost.get_no_ack_count.should eq 1
          vhost.deliver_get_count.should eq 1
          vhost.get_count.should eq 0
        end
      end
    end

    it "should count deliver and deliver_no_ack" do
      with_amqp_server do |s|
        with_channel(s) do |c|
          vhost = s.vhosts["/"]
          q = c.queue("q1")
          c.prefetch(1)

          2.times { q.publish "message" }
          q.subscribe(no_ack: false) { }
          q.subscribe(no_ack: true) { }

          s.update_stats_rates
          vhost.deliver_count.should eq 1
          vhost.deliver_no_ack_count.should eq 1
          vhost.deliver_get_count.should eq 2
        end
      end
    end
  end
end

# This module uses `extend MqttHelpers` to get access to MQTT test helpers
# (with_server, with_client_io, connect, publish, etc.)
module VHostByteRateSpecs
  extend MqttHelpers
  extend MqttMatchers

  # Sends data over the given protocol and returns the number of bytes
  # the vhost should have tracked. Uses exhaustive case (case...in) on
  # the Protocol enum so that adding a new protocol variant without
  # handling it here will cause a compile error.
  def self.send_and_receive_over_protocol(protocol, s, vhost)
    case protocol
    in LavinMQ::Server::Protocol::AMQP
      port = s.@listeners.keys.select(TCPServer).first.local_address.port
      conn = AMQP::Client.new(port: port, name: "byte-rate-spec").connect
      ch = conn.channel
      q = ch.queue("byte_rate_q")
      q.publish "amqp byte rate test message"
      received = Channel(Nil).new
      q.subscribe { received.send(nil) }
      received.receive
      conn.close(no_wait: false)
      wait_for { vhost.connections_dup.none?(LavinMQ::AMQP::Client) }
    in LavinMQ::Server::Protocol::MQTT
      with_client_io(s) do |sub_io|
        connect(sub_io)
        subscribe(sub_io, topic_filters: mk_topic_filters({"test/byte_rates", 0}))

        with_client_io(s) do |pub_io|
          connect(pub_io, client_id: "publisher")
          publish(pub_io, topic: "test/byte_rates", payload: "mqtt byte rate test".to_slice, qos: 0u8)
        end

        read_packet(sub_io)
      end
    end
  end

  describe LavinMQ::VHost do
    describe "Byte rate stats" do
      it "should track recv bytes at vhost level from AMQP publish" do
        with_server do |s|
          vhost = s.vhosts["/"]
          before = vhost.recv_oct_count

          port = s.@listeners.keys.select(TCPServer).first.local_address.port
          conn = AMQP::Client.new(port: port, name: "byte-rate-spec").connect
          ch = conn.channel
          q = ch.queue("byte_rate_q")
          q.publish "test message"
          wait_for { vhost.recv_oct_count > before }
          conn.close(no_wait: false)

          vhost.recv_oct_count.should be > before
        end
      end

      it "should track send bytes at vhost level from AMQP consume" do
        with_server do |s|
          vhost = s.vhosts["/"]

          port = s.@listeners.keys.select(TCPServer).first.local_address.port
          conn = AMQP::Client.new(port: port, name: "byte-rate-spec").connect
          ch = conn.channel
          q = ch.queue("byte_rate_q")
          q.publish "test message"

          before = vhost.send_oct_count
          received = Channel(Nil).new
          q.subscribe { received.send(nil) }
          received.receive

          wait_for { vhost.send_oct_count > before }
          vhost.send_oct_count.should be > before
          conn.close(no_wait: false)
        end
      end

      it "should track recv bytes at vhost level from MQTT publish" do
        with_server do |s|
          vhost = s.vhosts["/"]
          before = vhost.recv_oct_count

          with_client_io(s) do |io|
            connect(io)
            publish(io, topic: "test/byte_rates", payload: "mqtt payload".to_slice, qos: 0u8)
            pingpong(io)
          end

          vhost.recv_oct_count.should be > before
        end
      end

      it "should track send bytes at vhost level from MQTT deliver" do
        with_server do |s|
          vhost = s.vhosts["/"]

          with_client_io(s) do |io|
            connect(io)
            subscribe(io, topic_filters: mk_topic_filters({"test/byte_rates", 0}))

            before = vhost.send_oct_count

            with_client_io(s) do |pub_io|
              connect(pub_io, client_id: "publisher")
              publish(pub_io, topic: "test/byte_rates", payload: "mqtt payload".to_slice, qos: 0u8)
            end

            read_packet(io)

            wait_for { vhost.send_oct_count > before }
            vhost.send_oct_count.should be > before
          end
        end
      end

      # This test exercises every protocol (via exhaustive case...in on the
      # Protocol enum) and verifies that vhost-level byte counts are at least
      # as large as the sum of per-connection byte counts. If a new protocol
      # is added to the enum without handling it in send_and_receive_over_protocol,
      # this spec will fail to compile.
      it "all protocols should aggregate byte counts to vhost level" do
        with_server do |s|
          vhost = s.vhosts["/"]

          LavinMQ::Server::Protocol.each do |protocol|
            send_and_receive_over_protocol(protocol, s, vhost)
          end

          wait_for { vhost.connections_dup.empty? || vhost.connections_dup.all? { |c| c.recv_oct_count > 0 } }

          conn_recv_sum = vhost.connections_dup.sum(&.recv_oct_count)
          conn_send_sum = vhost.connections_dup.sum(&.send_oct_count)

          vhost.recv_oct_count.should be >= conn_recv_sum
          vhost.send_oct_count.should be >= conn_send_sum
          # Verify we actually tracked something
          vhost.recv_oct_count.should be > 0
          vhost.send_oct_count.should be > 0
        end
      end
    end
  end
end
