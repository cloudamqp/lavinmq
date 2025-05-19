require "./spec_helper"
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
        wait_for { s.connections.sum(&.channels.size).zero? }

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
          vhost.get_no_ack_count.should eq(0)
          q = c.queue("q1")
          q.publish "not acked message"

          q.get(true).not_nil!
          s.update_stats_rates
          vhost.get_no_ack_count.should eq(1)
          vhost.deliver_get_count.should eq(1)
          vhost.get_count.should eq(0)
        end
      end
    end
  end
end
