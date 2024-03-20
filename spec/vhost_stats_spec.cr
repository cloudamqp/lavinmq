require "./spec_helper"
require "../src/lavinmq/config"

describe LavinMQ::VHost do
  describe "Stats" do
    it "should update queue data" do
      vhost = Server.vhosts["/"]
      Server.update_stats_rates

      initial_declared_queues = vhost.queue_declared_count
      initial_deleted_queues = vhost.queue_deleted_count
      Server.vhosts["/"].declare_queue("this_queue_should_not_exist", false, false)
      Server.update_stats_rates

      vhost.queue_declared_count.should eq(initial_declared_queues + 1)
      vhost.queue_deleted_count.should eq initial_deleted_queues
      Server.vhosts["/"].delete_queue("this_queue_should_not_exist")
      Server.update_stats_rates

      vhost.queue_declared_count.should eq(initial_declared_queues + 1)
      vhost.queue_deleted_count.should eq(initial_deleted_queues + 1)
    end

    it "should not delete stats when connection is closed" do
      vhost = Server.vhosts["/"]
      wait_for { Server.connections.sum(&.channels.size).zero? }

      Server.update_stats_rates
      initial_channels_created = vhost.channel_created_count
      initial_channels_closed = vhost.channel_closed_count

      with_channel do |ch|
        Server.update_stats_rates

        vhost.channel_created_count.should eq(initial_channels_created + 1)
        vhost.channel_closed_count.should eq(initial_channels_closed)
        ch.close
      end

      Server.update_stats_rates
      vhost.channel_created_count.should eq(initial_channels_created + 1)
      vhost.channel_closed_count.should eq(initial_channels_closed + 1)
    end

    it "should count connections open / closed once" do
      vhost = Server.vhosts["/"]
      initial_connections_created = vhost.connection_created_count
      initial_connections_closed = vhost.connection_closed_count

      with_channel do
        Server.update_stats_rates
        vhost.connection_created_count.should eq(initial_connections_created + 1)
        vhost.connection_closed_count.should eq(initial_connections_closed)
      end

      wait_for { vhost.@connections.empty? }

      Server.update_stats_rates
      vhost.connection_created_count.should eq(initial_connections_created + 1)
      vhost.connection_closed_count.should eq(initial_connections_closed + 1)
    end
  end
end
