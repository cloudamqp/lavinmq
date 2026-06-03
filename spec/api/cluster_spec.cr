require "../spec_helper"

DR_SPEC_PREFIX = "drspec"

private def enable_dr_clustering
  LavinMQ::Config.instance.clustering = true
  LavinMQ::Config.instance.clustering_etcd_endpoints = "localhost:12379"
  LavinMQ::Config.instance.clustering_etcd_prefix = DR_SPEC_PREFIX
end

# DR promotion/demotion front door: the /api/cluster/dr endpoint drives the
# region's {prefix}/upstream_etcd key so failover doesn't need etcdctl surgery.
describe LavinMQ::HTTP::ClusterController, tags: "etcd" do
  add_etcd_around_each

  it "promotes, demotes and reverts a region via /api/cluster/dr" do
    with_http_server do |http, _|
      enable_dr_clustering
      etcd = LavinMQ::Etcd.new("localhost:12379")
      etcd.del("#{DR_SPEC_PREFIX}/upstream_etcd")

      # Promote: set the "primary" sentinel.
      resp = http.put("/api/cluster/dr", body: {upstream_etcd: "primary"}.to_json)
      resp.status_code.should eq 204
      etcd.get("#{DR_SPEC_PREFIX}/upstream_etcd").should eq "primary"

      body = JSON.parse(http.get("/api/cluster/dr").body)
      body["primary"].as_bool.should be_true
      body["effective_upstream"].as_s.should eq ""

      # Demote: set foreign etcd endpoints.
      resp = http.put("/api/cluster/dr", body: {upstream_etcd: "etcd-a:2379"}.to_json)
      resp.status_code.should eq 204
      etcd.get("#{DR_SPEC_PREFIX}/upstream_etcd").should eq "etcd-a:2379"

      body = JSON.parse(http.get("/api/cluster/dr").body)
      body["primary"].as_bool.should be_false
      body["effective_upstream"].as_s.should eq "etcd-a:2379"

      # Revert to the config fallback (empty -> primary).
      resp = http.delete("/api/cluster/dr")
      resp.status_code.should eq 204
      etcd.get("#{DR_SPEC_PREFIX}/upstream_etcd").should be_nil
      JSON.parse(http.get("/api/cluster/dr").body)["primary"].as_bool.should be_true
    end
  end

  it "rejects a PUT body without upstream_etcd" do
    with_http_server do |http, _|
      enable_dr_clustering
      resp = http.put("/api/cluster/dr", body: {foo: "bar"}.to_json)
      resp.status_code.should eq 400
    end
  end

  it "returns 409 when clustering is not enabled" do
    with_http_server do |http, _|
      resp = http.get("/api/cluster/dr")
      resp.status_code.should eq 409
    end
  end
end
