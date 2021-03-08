require "../spec_helper"
require "../../src/avalanchemq/config"

describe AvalancheMQ::HTTP::NodesController do
  describe "GET /api/nodes" do
    it "should return nodes data" do
      response = get("/api/nodes")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
      data = body.as_a.first.as_h
      keys = ["connection_created", "connection_closed"]
      keys.each do |key|
        data.has_key?(key).should be_true
      end
    end

    it "should update queue data" do
      s.update_stats_rates

      response = get("/api/nodes")
      body = JSON.parse(response.body)
      data = body.as_a.first.as_h
      declared_queues = data["queue_declared"].as_i
      deleted_queues = data["queue_deleted"].as_i
      s.vhosts["/"].declare_queue("this_queue_should_not_exist", false, false)
      s.update_stats_rates

      response = get("/api/nodes")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
      data = body.as_a.first.as_h
      data["queue_declared"].as_i.should eq (declared_queues + 1)
      data["queue_deleted"].as_i.should eq deleted_queues
      s.vhosts["/"].delete_queue("this_queue_should_not_exist")
      s.update_stats_rates

      response = get("/api/nodes")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
      data = body.as_a.first.as_h
      data["queue_declared"].as_i.should eq (declared_queues + 1)
      data["queue_deleted"].as_i.should eq (deleted_queues + 1)
    end

    it "should not delete stats when connection is closed" do
      wait_for { s.connections.sum { |c| c.channels.size }.zero? }
      s.update_stats_rates

      response = get("/api/nodes")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      data = body.as_a.first.as_h
      channels_created = data["channel_created"].as_i
      channels_closed = data["channel_closed"].as_i
      with_channel do
        s.update_stats_rates

        response = get("/api/nodes")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        data = body.as_a.first.as_h
        data["channel_created"].as_i.should eq (channels_created + 1)
        data["channel_closed"].as_i.should eq (channels_closed)
      end
      s.update_stats_rates

      response = get("/api/nodes")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      data = body.as_a.first.as_h
      data["channel_created"].as_i.should eq (channels_created + 1)
      data["channel_closed"].as_i.should eq (channels_closed + 1)
    end

    it "should count connections open / closed once" do
      response = get("/api/nodes")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      data = body.as_a.first.as_h
      connections_created = data["connection_created"].as_i
      connections_closed = data["connection_closed"].as_i
      with_channel do
        s.update_stats_rates

        response = get("/api/nodes")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        data = body.as_a.first.as_h
        data["connection_created"].as_i.should eq (connections_created + 1)
        data["connection_closed"].as_i.should eq (connections_closed)
      end
      s.update_stats_rates

      response = get("/api/nodes")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      data = body.as_a.first.as_h
      data["connection_created"].as_i.should eq (connections_created + 1)
      data["connection_closed"].as_i.should eq (connections_closed + 1)
    end
  end
end
