require "../spec_helper"
require "base64"
require "../../src/lavinmq/raft/backend"

private def with_raft_backend(&)
  config = LavinMQ::Config.instance
  config.clustering_bind = "127.0.0.1"
  config.clustering_raft_port = 0
  config.clustering_port = 0
  config.clustering_advertised_uri = "tcp://127.0.0.1:0"
  backend = LavinMQ::Raft::Backend.new(config)
  begin
    yield backend
  ensure
    backend.stop rescue nil
  end
end

private def with_raft_http_server(&)
  with_amqp_server do |s|
    with_raft_backend do |backend|
      h = LavinMQ::HTTP::Server.new(s, backend)
      begin
        addr = h.bind_tcp("::1", 0)
        spawn(name: "http listen") { h.listen }
        Fiber.yield
        yield({HTTPSpecHelper.new(addr), s})
      ensure
        h.close
      end
    end
  end
end

private def with_raft_metrics_server(&)
  with_amqp_server do |s|
    with_raft_backend do |backend|
      h = LavinMQ::HTTP::MetricsServer.new(s, backend)
      begin
        addr = h.bind_tcp("::1", 0)
        spawn(name: "metrics listen") { h.listen }
        Fiber.yield
        yield HTTPSpecHelper.new(addr)
      ensure
        h.close
      end
    end
  end
end

describe "raft HTTP surface authorization" do
  describe "metrics port" do
    it "serves read-only raft status without credentials" do
      with_raft_metrics_server do |http|
        response = http.get("/raft/status", {"Authorization" => ""})
        response.status_code.should eq 200
      end
    end

    it "does not expose mutating /raft/admin routes" do
      with_raft_metrics_server do |http|
        response = http.post("/raft/admin/promote_learner/99", {"Authorization" => ""})
        response.status_code.should eq 404
      end
    end
  end

  describe "main HTTP server" do
    it "requires authentication for raft status" do
      with_raft_http_server do |http, _s|
        response = http.get("/raft/status", {"Authorization" => ""})
        response.status_code.should eq 401
      end
    end

    it "serves raft status to any authenticated user" do
      with_raft_http_server do |http, _s|
        response = http.get("/raft/status")
        response.status_code.should eq 200
      end
    end

    it "refuses /raft/admin to a non-administrator" do
      with_raft_http_server do |http, s|
        s.users.create("mon", "pw", [LavinMQ::Tag::Monitoring])
        auth = "Basic " + Base64.strict_encode("mon:pw")
        response = http.post("/raft/admin/promote_learner/99", {"Authorization" => auth})
        response.status_code.should eq 403
      end
    end

    it "allows /raft/admin for an administrator" do
      with_raft_http_server do |http, _s|
        # guest is an administrator; 400 (unknown learner) proves the request
        # passed the guard and reached the admin handler.
        response = http.post("/raft/admin/promote_learner/99")
        response.status_code.should eq 400
      end
    end
  end
end
