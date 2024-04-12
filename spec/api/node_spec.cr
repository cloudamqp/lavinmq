require "../spec_helper"
require "../../src/lavinmq/config"

describe LavinMQ::HTTP::NodesController do
  describe "GET /api/nodes" do
    it "should return nodes data" do
      response = get("/api/nodes")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
      data = body.as_a.first.as_h
      keys = ["connection_created", "connection_closed", "messages_ready", "messages_unacknowledged"]
      keys.each do |key|
        data.has_key?(key).should be_true
      end
    end
  end
end
