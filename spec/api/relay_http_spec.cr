require "../spec_helper"

# The barebones HTTP API a DR relay serves instead of the full management API:
# follower Prometheus metrics, a readiness /health, and the DR control routes.
describe "LavinMQ::HTTP::Server.relay_api", tags: "etcd" do
  add_etcd_around_each

  it "serves /health (readiness), /metrics and /api/cluster/dr" do
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

    # /api/cluster/dr is served locally (DR control during failover)
    resp = HTTP::Client.get("#{base}/api/cluster/dr")
    resp.status_code.should eq 200
    JSON.parse(resp.body)["primary"]?.should_not be_nil

    # unknown paths -> 404 (no full management API)
    HTTP::Client.get("#{base}/api/queues").status_code.should eq 404
  ensure
    server.try &.close
  end
end
