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
      data["connection_created"].as_i.should eq 0.0
      data["connection_closed"].as_i.should eq 0.0
    end

    it "should update queue data" do
      s.vhosts["/"].declare_queue("q0", false, false)
      sleep 5.1 # Let stats_loop run once
      response = get("/api/nodes")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
      data = body.as_a.first.as_h
      data["queue_declared"].as_i.should eq 1
      data["queue_deleted"].as_i.should eq 0
      s.vhosts["/"].delete_queue("q0")
      sleep 5.1
      response = get("/api/nodes")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
      data = body.as_a.first.as_h
      data["queue_declared"].as_i.should eq 1
      data["queue_deleted"].as_i.should eq 1
    end
  end
end
