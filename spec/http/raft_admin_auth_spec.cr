require "../spec_helper"
require "base64"
require "../../src/lavinmq/raft/backend"
require "../../src/lavinmq/http/handler/raft_admin_auth"

private CLUSTER_SECRET = "spec-cluster-secret"

private def write_clustering_password(secret : String = CLUSTER_SECRET) : Nil
  path = File.join(LavinMQ::Config.instance.data_dir, ".clustering_password")
  File.write(path, secret)
  File.chmod(path, 0o600)
end

private def basic(user : String, password : String) : String
  "Basic " + Base64.strict_encode("#{user}:#{password}")
end

private def raft_admin_ctx(path : String, auth : String? = nil) : HTTP::Server::Context
  headers = HTTP::Headers.new
  headers["Authorization"] = auth if auth
  request = HTTP::Request.new("POST", path, headers)
  response = HTTP::Server::Response.new(IO::Memory.new)
  HTTP::Server::Context.new(request, response)
end

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
      h = LavinMQ::HTTP::Server.new(s, s.amqp_server, s.mqtt_server, backend)
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

    it "refuses /raft/admin to management users, even administrators" do
      with_raft_http_server do |http, _s|
        write_clustering_password
        # guest is an administrator, but membership changes authenticate with
        # the clustering password, not the user database.
        response = http.post("/raft/admin/promote_learner/99")
        response.status_code.should eq 401
      end
    end

    it "refuses /raft/admin with a wrong clustering password" do
      with_raft_http_server do |http, _s|
        write_clustering_password
        response = http.post("/raft/admin/promote_learner/99",
          {"Authorization" => basic("raft", "not-the-secret")})
        response.status_code.should eq 401
      end
    end

    it "allows /raft/admin with the clustering password, any username" do
      with_raft_http_server do |http, _s|
        write_clustering_password
        # 400 (unknown learner) proves the request passed the guard and
        # reached the admin handler.
        response = http.post("/raft/admin/promote_learner/99",
          {"Authorization" => basic("whoever", CLUSTER_SECRET)})
        response.status_code.should eq 400
      end
    end
  end

  describe LavinMQ::HTTP::RaftAdminAuth do
    it "rejects the admin path while the password file is missing" do
      with_raft_backend do |backend|
        guard = LavinMQ::HTTP::RaftAdminAuth.new("/raft/admin/", backend)
        reached = false
        guard.next = ->(_ctx : HTTP::Server::Context) { reached = true }
        ctx = raft_admin_ctx("/raft/admin/add_server/5", basic("raft", CLUSTER_SECRET))
        guard.call(ctx)
        ctx.response.status_code.should eq 401
        reached.should be_false
      end
    end

    it "passes requests outside the prefix through untouched" do
      with_raft_backend do |backend|
        guard = LavinMQ::HTTP::RaftAdminAuth.new("/raft/admin/", backend)
        reached = false
        guard.next = ->(_ctx : HTTP::Server::Context) { reached = true }
        guard.call(raft_admin_ctx("/api/overview"))
        reached.should be_true
      end
    end
  end
end
