require "../spec_helper"

# The barebones HTTP API a DR relay serves on its externally bound management
# port instead of the full management API: only unauthenticated, read-only
# endpoints — follower Prometheus metrics and a readiness /health. The DR
# control routes are deliberately NOT exposed here (they mutate cluster state
# and this port has no auth); they live on the local internal unix socket.
describe "LavinMQ::HTTP::Server.relay_api", tags: "etcd" do
  add_etcd_around_each

  it "serves /health (readiness) and /metrics" do
    LavinMQ::Config.instance.clustering = true
    LavinMQ::Config.instance.clustering_etcd_endpoints = "localhost:12379"
    LavinMQ::Config.instance.clustering_etcd_prefix = "relayhttpspec"

    ready = false
    server = LavinMQ::HTTP::Server.relay_api(-> { ready })
    addr = server.bind_tcp("127.0.0.1", 0)
    spawn(name: "relay api spec listen") { server.listen }
    Fiber.yield
    base = "http://#{addr}"

    # /health reflects readiness
    HTTP::Client.get("#{base}/health").status_code.should eq 503
    ready = true
    resp = HTTP::Client.get("#{base}/health")
    resp.status_code.should eq 200
    resp.body.should contain "ok"

    # /metrics returns follower Prometheus metrics
    resp = HTTP::Client.get("#{base}/metrics")
    resp.status_code.should eq 200
    resp.body.should contain "lavinmq"

    # DR control is NOT reachable on the unauthenticated TCP API: a client that
    # can reach this port must not be able to promote/demote the region.
    HTTP::Client.get("#{base}/api/cluster/dr").status_code.should eq 404
    HTTP::Client.put("#{base}/api/cluster/dr",
      body: {upstream_etcd: "primary"}.to_json).status_code.should eq 404
    HTTP::Client.delete("#{base}/api/cluster/dr").status_code.should eq 404

    # unknown paths -> 404 (no full management API)
    HTTP::Client.get("#{base}/api/queues").status_code.should eq 404
  ensure
    server.try &.close
  end
end

# The DR control routes (promote/demote) are served on a local unix socket
# (filesystem-permission gated) instead of the unauthenticated TCP API.
describe "LavinMQ::HTTP::FollowerDRHandler over a unix socket", tags: "etcd" do
  add_etcd_around_each

  it "serves /api/cluster/dr (GET/PUT/DELETE) on a unix socket" do
    LavinMQ::Config.instance.clustering = true
    LavinMQ::Config.instance.clustering_etcd_endpoints = "localhost:12379"
    LavinMQ::Config.instance.clustering_etcd_prefix = "relaysockspec"
    etcd = LavinMQ::Etcd.new("localhost:12379")
    etcd.del("relaysockspec/upstream_etcd")

    sock_path = File.tempname("lavinmqctl", ".sock")
    server = ::HTTP::Server.new([LavinMQ::HTTP::FollowerDRHandler.new] of ::HTTP::Handler) do |context|
      context.response.status_code = 503
    end
    server.bind_unix(sock_path)
    spawn(name: "dr sock spec listen") { server.listen }
    Fiber.yield

    client = -> { HTTP::Client.new(UNIXSocket.new(sock_path)) }

    # Promote via PUT, read it back via GET, revert via DELETE.
    resp = client.call.put("/api/cluster/dr", body: {upstream_etcd: "primary"}.to_json)
    resp.status_code.should eq 204
    etcd.get("relaysockspec/upstream_etcd").should eq "primary"

    body = JSON.parse(client.call.get("/api/cluster/dr").body)
    body["primary"].as_bool.should be_true

    client.call.delete("/api/cluster/dr").status_code.should eq 204
    etcd.get("relaysockspec/upstream_etcd").should be_nil

    # A PUT without upstream_etcd is rejected.
    client.call.put("/api/cluster/dr", body: {foo: "bar"}.to_json).status_code.should eq 400

    # Non-DR paths fall through to the 503 follower fallback.
    client.call.get("/api/queues").status_code.should eq 503
  ensure
    server.try &.close
    File.delete?(sock_path) if sock_path
  end
end
