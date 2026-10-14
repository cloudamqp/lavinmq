require "../spec_helper"
require "../../src/lavinmq/clustering_v2"
require "http/client"
require "amqp-client"

private def wait_until(timeout = 5.seconds, interval = 10.milliseconds, &block : -> Bool) : Bool
  deadline = Time.instant + timeout
  until block.call
    return false if Time.instant >= deadline
    sleep interval
  end
  true
end

private def auth_headers : HTTP::Headers
  HTTP::Headers{
    "Authorization" => "Basic #{Base64.strict_encode("guest:guest")}",
    "Content-Type"  => "application/json",
  }
end

describe LavinMQ::ClusteringV2::IngressRouter do
  it "serves read-only HTTP API and Prometheus /metrics locally from Hot-Standby followers while reverse-proxying mutations and /api/connections to the leader" do
    base_dir = File.tempname("lavinmq_cv2_http_ingress")
    Dir.mkdir_p(base_dir)
    nodes = Array(LavinMQ::ClusteringV2::ClusterNode).new

    begin
      infos = (1_u64..3_u64).map do |id|
        ndir = File.join(base_dir, "node#{id}")
        cn = LavinMQ::ClusteringV2::ClusterNode.new(ndir, id, secret: "test-secret")
        cn.raft.heartbeat_interval = 20.milliseconds
        cn.raft.election_timeout_min = 90.milliseconds
        cn.raft.election_timeout_max = 160.milliseconds
        cn.raft.leader_lease_timeout = 300.milliseconds
        nodes << cn
        cn.bind
      end

      nodes.each(&.raft.configure_cluster(infos))
      nodes[0].raft.trigger_election!

      wait_until { nodes.count(&.raft.leader?) == 1 }.should be_true
      leader = nodes.find!(&.raft.leader?)
      follower = nodes.reject(&.raft.leader?).first

      # 1. Issue a mutating PUT /api/queues/%2F/proxied_orders to the FOLLOWER
      #    It must be transparently reverse-proxied to the leader, replicated back
      #    to the follower's hot-standby state, and marked with X-LavinMQ-Proxied-To-Leader.
      put_resp = HTTP::Client.put(
        "http://127.0.0.1:#{follower.router.http_port}/api/queues/%2F/proxied_orders",
        headers: auth_headers,
        body: %({"durable":true})
      )
      {201, 204}.should contain(put_resp.status_code)
      put_resp.headers["X-LavinMQ-Cluster-Role"]?.should eq("follower")
      put_resp.headers["X-LavinMQ-Proxied-To-Leader"]?.should eq("true")
      put_resp.headers["X-LavinMQ-Leader-Id"]?.should eq(leader.raft.node_id.to_s)
      follower.router.http_router.proxied_to_leader_count.get.should be >= 1_u64

      # Publish 2 messages to "proxied_orders" on the leader and wait for replication
      q_leader = leader.server.vhosts["/"].queue("proxied_orders")
      2.times do |i|
        msg = LavinMQ::Message.new(
          RoughTime.unix_ms,
          "",
          "proxied_orders",
          LavinMQ::AMQP::Properties.new(delivery_mode: 2_u8),
          "msg-#{i}".bytesize.to_u64,
          IO::Memory.new("msg-#{i}")
        )
        q_leader.publish(msg)
      end
      leader.replicator.wait_for_followers

      wait_until do
        follower.server.vhosts["/"].queue?("proxied_orders").try(&.message_count) == 2
      end.should be_true

      # 2. Query read-only GET /api/queues, /api/vhosts, /api/overview, and /metrics on the FOLLOWER
      #    They must be served locally from the follower's hot-standby state without proxying to the leader.
      prev_proxied = follower.router.http_router.proxied_to_leader_count.get

      queues_resp = HTTP::Client.get(
        "http://127.0.0.1:#{follower.router.http_port}/api/queues",
        headers: auth_headers
      )
      queues_resp.status_code.should eq(200)
      queues_resp.headers["X-LavinMQ-Cluster-Role"]?.should eq("follower")
      queues_resp.headers["X-LavinMQ-Proxied-To-Leader"]?.should be_nil
      queues_resp.headers["X-LavinMQ-Node-Id"]?.should eq(follower.raft.node_id.to_s)

      queues_json = JSON.parse(queues_resp.body).as_a
      order_q_json = queues_json.find! { |q| q["name"].as_s == "proxied_orders" }
      order_q_json["messages"].as_i.should eq(2)

      overview_resp = HTTP::Client.get(
        "http://127.0.0.1:#{follower.router.http_port}/api/overview",
        headers: auth_headers
      )
      overview_resp.status_code.should eq(200)
      overview_resp.headers["X-LavinMQ-Proxied-To-Leader"]?.should be_nil

      metrics_resp = HTTP::Client.get(
        "http://127.0.0.1:#{follower.router.http_port}/metrics",
        headers: auth_headers
      )
      metrics_resp.status_code.should eq(200)
      metrics_resp.headers["X-LavinMQ-Proxied-To-Leader"]?.should be_nil

      # Verify none of those read-only requests were proxied to the leader
      follower.router.http_router.proxied_to_leader_count.get.should eq(prev_proxied)
      follower.router.http_router.local_served_count.get.should be >= 3_u64

      # 3. Query GET /api/connections on the FOLLOWER -> must reverse-proxy to the leader
      conns_resp = HTTP::Client.get(
        "http://127.0.0.1:#{follower.router.http_port}/api/connections",
        headers: auth_headers
      )
      conns_resp.status_code.should eq(200)
      conns_resp.headers["X-LavinMQ-Proxied-To-Leader"]?.should eq("true")
      follower.router.http_router.proxied_to_leader_count.get.should eq(prev_proxied + 1)
    ensure
      nodes.each(&.close)
      FileUtils.rm_rf(base_dir)
    end
  end

  it "keeps AMQP listener sockets permanently open across role transitions and transparently Layer-4 proxies follower AMQP connections to the leader" do
    base_dir = File.tempname("lavinmq_cv2_l4_proxy")
    Dir.mkdir_p(base_dir)
    nodes = Array(LavinMQ::ClusteringV2::ClusterNode).new

    begin
      infos = (1_u64..3_u64).map do |id|
        ndir = File.join(base_dir, "node#{id}")
        cn = LavinMQ::ClusteringV2::ClusterNode.new(ndir, id, secret: "test-secret")
        cn.raft.heartbeat_interval = 20.milliseconds
        cn.raft.election_timeout_min = 90.milliseconds
        cn.raft.election_timeout_max = 160.milliseconds
        cn.raft.leader_lease_timeout = 200.milliseconds
        cn.raft.leader_stickiness_window = 50.milliseconds
        nodes << cn
        cn.bind
      end

      nodes.each(&.raft.configure_cluster(infos))
      nodes[0].raft.trigger_election!

      wait_until { nodes.count(&.raft.leader?) == 1 }.should be_true
      leader = nodes.find!(&.raft.leader?)
      followers = nodes.reject(&.raft.leader?)
      target_follower = followers.first
      follower_amqp_port = target_follower.router.amqp_port

      # 1. Connect an AMQP client directly to the FOLLOWER's permanent AMQP port.
      #    IngressRouter splices the TCP stream at Layer-4 to the leader.
      AMQP::Client.start("amqp://guest:guest@127.0.0.1:#{follower_amqp_port}") do |conn|
        ch = conn.channel
        ch.confirm_select
        q = ch.queue("l4_spliced_q", durable: true)
        q.publish_confirm("replicated-via-follower-l4-splice", props: AMQP::Client::Properties.new(delivery_mode: 2_u8)).should be_true
      end

      # Verify the queue and message were written on the leader and replicated to all followers
      wait_until do
        nodes.all? { |n| n.server.vhosts["/"].queue?("l4_spliced_q").try(&.message_count) == 1 }
      end.should be_true

      # 2. Isolate the leader and promote target_follower to Leader in-process.
      #    Verify its AMQP listener port never changes or closes, and now serves AMQP directly!
      leader.raft.set_isolated(true)
      # Pause the other follower's elections so target_follower deterministically wins
      followers[1].raft.election_timeout_min = 2.seconds
      followers[1].raft.election_timeout_max = 3.seconds
      sleep 60.milliseconds
      target_follower.raft.trigger_election!

      wait_until { target_follower.raft.leader? }.should be_true
      target_follower.router.amqp_port.should eq(follower_amqp_port)

      # Connect to the exact same port on target_follower (now Leader) and consume the message
      consumed_body : String? = nil
      AMQP::Client.start("amqp://guest:guest@127.0.0.1:#{follower_amqp_port}") do |conn|
        ch = conn.channel
        q = ch.queue("l4_spliced_q", durable: true)
        if msg = q.get(no_ack: false)
          consumed_body = msg.body_io.to_s
          msg.ack
        end
      end
      consumed_body.should eq("replicated-via-follower-l4-splice")
    ensure
      nodes.each(&.close)
      FileUtils.rm_rf(base_dir)
    end
  end
end
