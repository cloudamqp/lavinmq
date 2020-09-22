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
      sleep 5.1 # Let stats_loop run once
      response = get("/api/nodes")

      body = JSON.parse(response.body)
      data = body.as_a.first.as_h
      declared_queues = data["queue_declared"].as_i
      deleted_queues = data["queue_deleted"].as_i

      s.vhosts["/"].declare_queue("q0", false, false)
      sleep 5.1 # Let stats_loop run once
      response = get("/api/nodes")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
      data = body.as_a.first.as_h
      data["queue_declared"].as_i.should eq (declared_queues + 1)
      data["queue_deleted"].as_i.should eq deleted_queues
      s.vhosts["/"].delete_queue("q0")
      sleep 5.1
      response = get("/api/nodes")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
      data = body.as_a.first.as_h
      data["queue_declared"].as_i.should eq (declared_queues + 1)
      data["queue_deleted"].as_i.should eq (deleted_queues + 1)
    end
  end
end
