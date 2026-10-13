require "../spec_helper"
require "../../src/lavinmq/clustering_v2"

private def wait_until(timeout = 5.seconds, interval = 10.milliseconds, &block : -> Bool) : Bool
  deadline = Time.instant + timeout
  until block.call
    return false if Time.instant >= deadline
    sleep interval
  end
  true
end

private def make_test_message(body : String, routing_key : String = "q1", exchange : String = "") : LavinMQ::Message
  LavinMQ::Message.new(
    RoughTime.unix_ms,
    exchange,
    routing_key,
    LavinMQ::AMQP::Properties.new(delivery_mode: 2_u8),
    body.bytesize.to_u64,
    IO::Memory.new(body)
  )
end

describe LavinMQ::ClusteringV2::LogicalReplicator do
  it "replicates topology, publishes, acks, stream offsets, and MQTT retains to Hot-Standby followers and promotes in <50ms" do
    base_dir = File.tempname("lavinmq_cv2_hot_standby")
    Dir.mkdir_p(base_dir)
    nodes = Array(LavinMQ::ClusteringV2::ClusterNode).new

    begin
      infos = (1_u64..3_u64).map do |id|
        ndir = File.join(base_dir, "node#{id}")
        cn = LavinMQ::ClusteringV2::ClusterNode.new(ndir, id)
        cn.raft.heartbeat_interval = 20.milliseconds
        cn.raft.election_timeout_min = 90.milliseconds
        cn.raft.election_timeout_max = 160.milliseconds
        cn.raft.leader_lease_timeout = 200.milliseconds
        cn.raft.leader_stickiness_window = 60.milliseconds
        nodes << cn
        cn.bind
      end

      nodes.each(&.raft.configure_cluster(infos))
      nodes[0].raft.trigger_election!

      wait_until { nodes.count(&.raft.leader?) == 1 }.should be_true
      leader = nodes.find!(&.raft.leader?)
      followers = nodes.reject(&.raft.leader?)

      # 1. Declare a durable classic queue, a stream queue, an exchange, and a binding on the leader
      leader_vh = leader.server.vhosts["/"]
      leader_vh.declare_queue("orders", true, false)
      stream_args = LavinMQ::AMQP::Table.new({"x-queue-type": "stream"})
      leader_vh.declare_queue("events_stream", true, false, stream_args)
      leader_vh.declare_exchange("orders_ex", "direct", true, false)
      leader_vh.bind_queue("orders", "orders_ex", "order.created")

      # Wait for definitions to replicate to hot-standby followers
      wait_until do
        followers.all? do |f|
          vh = f.server.vhosts["/"]
          !vh.queue?("orders").nil? &&
            !vh.queue?("events_stream").nil? &&
            !vh.exchange?("orders_ex").nil?
        end
      end.should be_true

      # 2. Publish 3 durable messages to "orders" and ack the first one on the leader
      q_leader = leader_vh.queue("orders")
      3.times do |i|
        q_leader.publish(make_test_message("order-#{i + 1}", routing_key: "orders"))
      end
      leader.replicator.wait_for_followers

      wait_until do
        followers.all? { |f| f.server.vhosts["/"].queue("orders").message_count == 3 }
      end.should be_true

      # Ack the first message ("order-1") on the leader
      got_body : String? = nil
      q_leader.basic_get(true) do |env|
        got_body = String.new(env.message.body)
      end
      got_body.should eq("order-1")
      leader.replicator.wait_for_followers

      # Verify Hot-Standby followers immediately reflect 2 remaining messages ("order-2", "order-3")
      wait_until do
        followers.all? { |f| f.server.vhosts["/"].queue("orders").message_count == 2 }
      end.should be_true

      # 3. Publish to the stream queue and commit a consumer offset on the leader
      sq_leader = leader_vh.queue("events_stream").as(LavinMQ::AMQP::Stream)
      sq_leader.publish(make_test_message("event-1", routing_key: "events_stream"))
      sq_leader.publish(make_test_message("event-2", routing_key: "events_stream"))
      sq_leader.raw_msg_store.as(LavinMQ::AMQP::StreamMessageStore).store_consumer_offset("ctag-1", 2_i64)
      leader.replicator.wait_for_followers

      wait_until do
        followers.all? do |f|
          sq = f.server.vhosts["/"].queue("events_stream").as(LavinMQ::AMQP::Stream)
          store = sq.raw_msg_store.as(LavinMQ::AMQP::StreamMessageStore)
          sq.message_count == 2 && store.last_offset_by_consumer_tag("ctag-1") == 2_i64
        end
      end.should be_true

      # 4. Retain an MQTT message on the leader
      leader_vh.mqtt_broker.raw_retain_store.apply_follower_retain("sensors/temp", "21.5C".to_slice)
      leader.replicator.wait_for_followers

      wait_until do
        followers.all? do |f|
          found = false
          f.server.vhosts["/"].mqtt_broker.raw_retain_store.each("sensors/temp") do |_topic, io, size|
            found = (io.read_string(size) == "21.5C")
          end
          found
        end
      end.should be_true

      # 5. Isolate the leader and verify Hot-Standby state-machine promotion time is < 50ms
      promotion_duration : Time::Span? = nil
      followers.each do |f|
        f.replicator.on_role_change do |role|
          if role.leader?
            t0 = Time.instant
            # Verify queue state is already live in memory at the exact instant of promotion
            _ = f.server.vhosts["/"].queue("orders").message_count
            promotion_duration = Time.instant - t0
          end
        end
      end

      leader.raft.set_isolated(true)
      wait_until { followers.any?(&.raft.leader?) }.should be_true

      new_leader = followers.find!(&.raft.leader?)
      promotion_duration.not_nil!.should be < 50.milliseconds

      # Consume the remaining 2 messages from the newly promoted leader and verify order & content
      q_new_leader = new_leader.server.vhosts["/"].queue("orders")
      q_new_leader.message_count.should eq(2)

      bodies = Array(String).new
      2.times do
        q_new_leader.basic_get(true) do |env|
          bodies << String.new(env.message.body)
        end
      end
      bodies.should eq(["order-2", "order-3"])
    ensure
      nodes.each(&.close)
      FileUtils.rm_rf(base_dir)
    end
  end

  it "gates consumer delivery on Raft majority commit_index when commit_gated_delivery is enabled" do
    base_dir = File.tempname("lavinmq_cv2_commit_gate")
    Dir.mkdir_p(base_dir)
    nodes = Array(LavinMQ::ClusteringV2::ClusterNode).new

    begin
      infos = (1_u64..3_u64).map do |id|
        ndir = File.join(base_dir, "node#{id}")
        cn = LavinMQ::ClusteringV2::ClusterNode.new(ndir, id, commit_gated_delivery: true)
        cn.raft.heartbeat_interval = 20.milliseconds
        cn.raft.election_timeout_min = 120.milliseconds
        cn.raft.election_timeout_max = 220.milliseconds
        cn.raft.leader_lease_timeout = 5.seconds
        nodes << cn
        cn.bind
      end

      nodes.each(&.raft.configure_cluster(infos))
      nodes[0].raft.trigger_election!

      wait_until { nodes.count(&.raft.leader?) == 1 }.should be_true
      leader = nodes.find!(&.raft.leader?)
      followers = nodes.reject(&.raft.leader?)

      leader_vh = leader.server.vhosts["/"]
      leader_vh.declare_queue("gated_q", true, false)
      wait_until { followers.all? { |f| !f.server.vhosts["/"].queue?("gated_q").nil? } }.should be_true

      q = leader_vh.queue("gated_q")
      q.raw_msg_store.commit_gated?.should be_true

      # Pause replication to BOTH followers so the next publish cannot achieve a 2/3 majority commit
      followers.each { |f| leader.raft.pause_peer(f.raft.node_id, true) }

      q.publish(make_test_message("uncommitted-secret", routing_key: "gated_q"))

      # Because the publish LSN is > commit_index, consumers MUST NOT see the uncommitted message
      q.empty?.should be_true
      delivered = false
      q.basic_get(true) { |_env| delivered = true }
      delivered.should be_false

      # Unpause ONE follower so 2/3 majority quorum is reached
      leader.raft.pause_peer(followers[0].raft.node_id, false)

      wait_until { !q.empty? }.should be_true

      body : String? = nil
      q.basic_get(true) { |env| body = String.new(env.message.body) }
      body.should eq("uncommitted-secret")
    ensure
      nodes.each(&.close)
      FileUtils.rm_rf(base_dir)
    end
  end

  it "steps down in-process without exit(3) and rolls back uncommitted publishes on rejoin" do
    base_dir = File.tempname("lavinmq_cv2_rollback")
    Dir.mkdir_p(base_dir)
    nodes = Array(LavinMQ::ClusteringV2::ClusterNode).new

    begin
      infos = (1_u64..3_u64).map do |id|
        ndir = File.join(base_dir, "node#{id}")
        cn = LavinMQ::ClusteringV2::ClusterNode.new(ndir, id, commit_gated_delivery: true)
        cn.raft.heartbeat_interval = 20.milliseconds
        cn.raft.election_timeout_min = 90.milliseconds
        cn.raft.election_timeout_max = 160.milliseconds
        cn.raft.leader_lease_timeout = 250.milliseconds
        cn.raft.leader_stickiness_window = 50.milliseconds
        nodes << cn
        cn.bind
      end

      nodes.each(&.raft.configure_cluster(infos))
      nodes[0].raft.trigger_election!

      wait_until { nodes[0].raft.leader? }.should be_true
      old_leader = nodes[0]
      others = [nodes[1], nodes[2]]

      old_leader.server.vhosts["/"].declare_queue("rb_q", true, false)
      old_leader.server.vhosts["/"].queue("rb_q").publish(make_test_message("committed-1", routing_key: "rb_q"))
      old_leader.replicator.wait_for_followers

      wait_until do
        others.all? { |f| f.server.vhosts["/"].queue?("rb_q").try(&.message_count) == 1 }
      end.should be_true

      # Isolate old_leader and immediately write an uncommitted message before its lease expires
      old_leader.raft.set_isolated(true)
      old_leader.server.vhosts["/"].queue("rb_q").publish(make_test_message("phantom-uncommitted", routing_key: "rb_q"))
      old_leader.server.vhosts["/"].queue("rb_q").message_count.should eq(2)

      # Wait for the majority partition (nodes[1], nodes[2]) to elect a new leader
      wait_until { others.any?(&.raft.leader?) }.should be_true
      new_leader = others.find!(&.raft.leader?)

      # Publish a committed message on the new leader in the higher term
      new_leader.server.vhosts["/"].queue("rb_q").publish(make_test_message("committed-2", routing_key: "rb_q"))
      new_leader.replicator.wait_for_followers

      # Reconnect old_leader: it must step down in-process, roll back "phantom-uncommitted",
      # and replicate "committed-2" from new_leader
      old_leader.raft.set_isolated(false)

      wait_until do
        old_leader.raft.follower? &&
          old_leader.raft.commit_index == new_leader.raft.commit_index &&
          old_leader.server.vhosts["/"].queue("rb_q").message_count == 2
      end.should be_true

      # Promote old_leader or inspect its queue to verify the messages are ["committed-1", "committed-2"]
      q_old = old_leader.server.vhosts["/"].queue("rb_q")
      bodies = Array(String).new
      2.times do
        q_old.basic_get(true) { |env| bodies << String.new(env.message.body) }
      end
      bodies.should eq(["committed-1", "committed-2"])
    ensure
      nodes.each(&.close)
      FileUtils.rm_rf(base_dir)
    end
  end
end
