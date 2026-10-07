require "../spec_helper"
require "../../src/lavinmq/config"

describe LavinMQ::HTTP::NodesController do
  describe "GET /api/nodes" do
    it "should return nodes data" do
      with_http_server do |http, _|
        response = http.get("/api/nodes")
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

    it "should not include GC stats" do
      with_http_server do |http, _|
        response = http.get("/api/nodes")
        response.status_code.should eq 200
        JSON.parse(response.body).as_a.first.as_h.has_key?("gc_stats").should be_false
      end
    end
  end

  describe "GET /api/nodes/gc_stats" do
    it "returns GC profiling stats" do
      with_http_server do |http, _|
        response = http.get("/api/nodes/gc_stats")
        response.status_code.should eq 200
        body = JSON.parse(response.body).as_h
        body.has_key?("gc_no").should be_true
        body.has_key?("heap_size").should be_true
      end
    end
  end

  describe "POST /api/nodes/gc_collect" do
    it "triggers garbage collection" do
      with_http_server do |http, _|
        before = GC.prof_stats.gc_no
        response = http.post("/api/nodes/gc_collect")
        response.status_code.should eq 204
        GC.prof_stats.gc_no.should be > before
      end
    end
  end

  describe "POST /api/nodes/:name/gc_collect" do
    it "triggers garbage collection for the current node" do
      with_http_server do |http, _|
        before = GC.prof_stats.gc_no
        response = http.post("/api/nodes/#{System.hostname}/gc_collect")
        response.status_code.should eq 204
        GC.prof_stats.gc_no.should be > before
      end
    end

    it "returns 404 for an unknown node" do
      with_http_server do |http, _|
        response = http.post("/api/nodes/unknown-node/gc_collect")
        response.status_code.should eq 404
      end
    end
  end
end
