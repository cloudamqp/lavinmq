require "../spec_helper"
require "../../src/lavinmq/clustering/controller"
require "../../src/lavinmq/clustering/raft/transport"

private def free_port : Int32
  s = TCPServer.new("127.0.0.1", 0)
  s.local_address.port
ensure
  s.try &.close
end

# A single node raft cluster behind the HTTP API
private def with_cluster_api(&)
  with_datadir do |dir|
    port = free_port
    config = LavinMQ::Config.new
    config.data_dir = dir
    config.clustering = true
    config.clustering_bind = "127.0.0.1"
    config.clustering_raft_port = port
    config.clustering_raft_advertised_address = "127.0.0.1:#{port}"
    config.clustering_peers = "127.0.0.1:#{port}"
    config.clustering_secret = "cluster-api-spec"
    config.clustering_election_timeout = 300
    config.clustering_heartbeat_interval = 50
    config.clustering_port = free_port
    config.clustering_advertised_uri = "tcp://127.0.0.1:#{config.clustering_port}"
    config.metrics_http_port = -1
    controller = LavinMQ::Clustering::RaftController.new(config)
    serving = Channel(Nil).new
    spawn(name: "cluster api spec") { controller.run { serving.send nil } }
    select
    when serving.receive
    when timeout(5.seconds)
      fail "single node cluster didn't elect itself"
    end
    begin
      with_amqp_server do |s|
        h = LavinMQ::HTTP::Server.new(s, s.amqp_server, s.mqtt_server, controller)
        addr = h.bind_tcp("127.0.0.1", 0)
        spawn(name: "http listen") { h.listen }
        Fiber.yield
        yield HTTPSpecHelper.new(addr), "127.0.0.1:#{port}"
        h.close
      end
    ensure
      controller.stop
    end
  end
end

describe LavinMQ::HTTP::ClusterController do
  it "returns 404 without clustering" do
    with_http_server do |http, _|
      http.get("/api/cluster").status_code.should eq 404
      http.post("/api/cluster/members", body: %({"address":"a:1"})).status_code.should eq 404
      http.post("/api/cluster/transfer-leadership", body: "{}").status_code.should eq 404
    end
  end

  it "returns 400 on the etcd backend" do
    config = LavinMQ::Config.instance
    config.clustering = true
    with_http_server do |http, _|
      http.get("/api/cluster").status_code.should eq 400
      http.delete("/api/cluster/members/a:1").status_code.should eq 400
    end
  ensure
    LavinMQ::Config.instance.clustering = false
  end

  it "requires an administrator" do
    with_cluster_api do |http, _|
      http.get("/api/cluster", headers: {"Authorization" => "Basic bm9ib2R5Om5vYm9keQ=="}).status_code.should eq 401
    end
  end

  it "reports the leader and the members" do
    with_cluster_api do |http, addr|
      response = http.get("/api/cluster")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body["leader"].as_s.should eq addr
      members = body["members"].as_a
      members.size.should eq 1
      members[0]["address"].as_s.should eq addr
      members[0]["node_id"].as_s.should_not be_empty
      members[0]["role"].as_s.should eq "voter"
      members[0]["leader"].as_bool.should be_true
      members[0]["caught_up"].as_bool.should be_true
    end
  end

  it "adds and removes a learner, and refuses the impossible with 409" do
    with_cluster_api do |http, addr|
      # Something answering the raft handshake, as clustering id 77 ("25")
      server = TCPServer.new("127.0.0.1", 0)
      learner = "127.0.0.1:#{server.local_address.port}"
      transport = LavinMQ::Clustering::Raft::TCPTransport.new("cluster-api-spec", 77, learner, Array(String).new,
        ->(_e : LavinMQ::Clustering::Raft::TransportEvent) { })
      spawn transport.listen(server)

      http.post("/api/cluster/members", body: "{}").status_code.should eq 400
      http.post("/api/cluster/members", body: %({"address":"127.0.0.1:1"})).status_code.should eq 409 # unreachable
      http.post("/api/cluster/members", body: %({"address":"#{learner}"})).status_code.should eq 201
      http.post("/api/cluster/members", body: %({"address":"#{learner}"})).status_code.should eq 409
      members = JSON.parse(http.get("/api/cluster").body)["members"].as_a
      member = members.find! { |m| m["address"] == learner }
      member["role"].as_s.should eq "learner"
      member["node_id"].as_s.should eq "25"
      # not in the ISR (never seen)
      http.post("/api/cluster/members/#{URI.encode_www_form(learner)}/promote").status_code.should eq 409
      http.post("/api/cluster/members/25/promote").status_code.should eq 409
      http.post("/api/cluster/members/127.0.0.1:9/promote").status_code.should eq 404
      http.delete("/api/cluster/members/25").status_code.should eq 204
      http.delete("/api/cluster/members/25").status_code.should eq 404
      http.delete("/api/cluster/members/#{URI.encode_www_form(addr)}").status_code.should eq 409 # the leader
    ensure
      transport.try &.close
    end
  end

  it "refuses to hand over leadership to a node that isn't an eligible voter" do
    with_cluster_api do |http, addr|
      http.post("/api/cluster/transfer-leadership", body: "{}").status_code.should eq 409
      http.post("/api/cluster/transfer-leadership", body: %({"target":"127.0.0.1:1"})).status_code.should eq 409
      http.post("/api/cluster/transfer-leadership", body: %({"target":"#{addr}"})).status_code.should eq 409
      # still the leader
      JSON.parse(http.get("/api/cluster").body)["leader"].as_s.should eq addr
    end
  end
end
