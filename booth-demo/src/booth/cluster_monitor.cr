require "http/client"
require "json"
require "wait_group"
require "./config"
require "./events"
require "./log"

module Booth
  class NodeStatus
    getter name : String
    getter host : String
    property? online = false
    property? leader = false
    property node_id : String?
    property term : Int64?
    property? has_leader : Bool?
    property voter : Bool?
    property in_isr : Bool?
    property caught_up : Bool?
    property lag_bytes : Int64?
    property offline_since : Time?
    property clients = 0
    # What the events last said about this node, so each change is told once
    property reported_online : Bool? = nil
    property reported_in_isr : Bool? = nil

    def initialize(@name, @host)
    end

    def state : String
      return "offline" unless online?
      return "leader" if leader?
      case in_isr
      when true  then "in_sync"
      when false then "syncing"
      else            "follower"
      end
    end

    def to_json(json : JSON::Builder)
      json.object do
        json.field "name", name
        json.field "host", host
        json.field "state", state
        json.field "online", online?
        json.field "leader", leader?
        json.field "node_id", node_id
        json.field "term", term
        json.field "voter", voter
        json.field "in_isr", in_isr
        json.field "caught_up", caught_up
        json.field "lag_bytes", lag_bytes
        json.field "offline_since", offline_since.try &.to_unix_ms
        json.field "clients", clients
      end
    end
  end

  # Polls every node for its own view (the metrics port isn't proxied to the
  # leader) and the cluster API for the leader's view, and turns changes into
  # events for the dashboard.
  class ClusterMonitor
    INTERVAL = 500.milliseconds

    getter nodes : Array(NodeStatus)
    getter backend = "unknown"
    getter term : Int64?
    getter leaderless_since : Time?
    getter overview = JSON::Any.new(nil)
    getter queues = Hash(String, Int64).new

    @polls = 0
    @last_leader : NodeStatus? = nil

    def initialize(@config : Config, @events : Events)
      @nodes = @config.nodes.map { |n| NodeStatus.new(n.name, n.host) }
    end

    def leader : NodeStatus?
      @nodes.find(&.leader?)
    end

    def node_for(host : String) : NodeStatus?
      @nodes.find { |n| n.host == host }
    end

    # Hosts to try connecting to, the leader first: a follower forwards
    # connections to the leader, but going there directly is one hop less.
    def hosts_leader_first : Array(String)
      online, offline = @nodes.partition(&.online?)
      online.sort_by! { |n| n.leader? ? 0 : 1 }
      (online + offline).map(&.host)
    end

    def online_hosts : Array(String)
      @nodes.select(&.online?).map(&.host)
    end

    # Node status comes from each node's own metrics, quickly; the cluster
    # API goes through the leader and can stall on a dead one, so it's
    # polled on its own and never delays noticing a node going away.
    def run : Nil
      spawn(name: "cluster api poll") { every(INTERVAL) { poll_api } }
      every(INTERVAL) { poll_nodes }
    end

    def poll : Nil
      poll_nodes
      poll_api
    end

    private def every(interval, &) : Nil
      loop do
        started = Time.instant
        begin
          yield
        rescue ex
          Log.warn(exception: ex) { "Cluster poll failed" }
        end
        elapsed = Time.instant - started
        sleep interval - elapsed if elapsed < interval
      end
    end

    def poll_nodes : Nil
      wg = WaitGroup.new(@nodes.size)
      @nodes.each do |node|
        spawn do
          poll_metrics(node)
        ensure
          wg.done
        end
      end
      wg.wait
      detect_changes
    end

    def poll_api : Nil
      poll_cluster
      poll_overview if (@polls += 1).odd?
      detect_changes
    end

    # Asks the node itself, so this works also while it has no leader
    private def poll_metrics(node : NodeStatus) : Nil
      body = http_get(node.host, @config.metrics_port, "/metrics?prefix=lavinmq", auth: false, timeout: 400.milliseconds)
      unless body
        node.online = false
        node.leader = false
        return
      end
      node.online = true
      metrics = Prometheus.parse(body)
      if is_leader = metrics["lavinmq_raft_is_leader"]?
        node.leader = is_leader == 1
        node.term = metrics["lavinmq_raft_term"]?.try &.to_i64
        node.has_leader = metrics["lavinmq_raft_has_leader"]? == 1
      else
        # etcd backend: only the leader reports its followers
        node.leader = metrics.has_key?("lavinmq_total_connected_followers")
        node.term = nil
      end
    end

    private def poll_cluster : Nil
      host = leader.try(&.host) || online_hosts.first?
      return unless host
      response = http_get_response(host, @config.http_port, "/api/cluster")
      return unless response
      case response.status_code
      when 200
        @backend = "raft"
        apply_raft_status(JSON.parse(response.body))
      when 400 # etcd backend
        @backend = "etcd"
        if body = http_get(host, @config.http_port, "/api/nodes")
          apply_etcd_followers(JSON.parse(body), host)
        end
      when 404
        @backend = "standalone"
      end
    rescue ex : JSON::ParseException
      Log.debug { "Bad cluster status: #{ex.message}" }
    end

    def apply_raft_status(status : JSON::Any) : Nil
      @term = status["term"]?.try &.as_i64?
      leader_host = status["leader"]?.try(&.as_s?).try { |a| self.class.address_host(a) }
      status["members"]?.try &.as_a.each do |m|
        host = self.class.address_host(m["address"].as_s)
        node = node_for(host) || @nodes.find { |n| n.node_id && n.node_id == m["node_id"]?.try &.as_s? }
        next unless node
        node.node_id = m["node_id"]?.try &.as_s?
        node.voter = m["role"]?.try &.as_s? == "voter"
        node.in_isr = m["in_isr"]?.try &.as_bool?
        node.caught_up = m["caught_up"]?.try &.as_bool?
      end
      # The leader's view wins over metrics that may be half a second old
      if leader_host && (lnode = node_for(leader_host)) && lnode.online?
        @nodes.each { |n| n.leader = n == lnode }
      end
    end

    def apply_etcd_followers(nodes_json : JSON::Any, leader_host : String) : Nil
      followers = nodes_json[0]?.try(&.["followers"]?).try(&.as_a?) || [] of JSON::Any
      @nodes.each do |n|
        next if n.host == leader_host
        f = followers.find { |x| self.class.address_host(x["remote_address"].as_s) == n.host }
        n.in_isr = !f.nil?
        n.lag_bytes = f.try &.["lag_in_bytes"]?.try &.as_i64?
        n.caught_up = n.lag_bytes.try &.zero?
      end
    end

    private def poll_overview : Nil
      host = leader.try(&.host) || online_hosts.first?
      return unless host
      if body = http_get(host, @config.http_port, "/api/overview")
        @overview = JSON.parse(body)
      end
      vhost = URI.encode_path_segment(@config.vhost)
      if body = http_get(host, @config.http_port, "/api/queues/#{vhost}?columns=name,messages")
        JSON.parse(body).as_a.each do |q|
          name = q["name"].as_s
          @queues[name] = q["messages"]?.try(&.as_i64?) || 0i64 if name.starts_with?("booth.")
        end
      end
    rescue ex : JSON::ParseException
      Log.debug { "Bad overview: #{ex.message}" }
    end

    private def detect_changes : Nil
      now = Time.utc
      @nodes.each { |node| detect_node_change(node, now) }
      detect_leader_change(now)
    end

    private def detect_node_change(node : NodeStatus, now : Time) : Nil
      was_online = node.reported_online
      if was_online && !node.online?
        node.offline_since = now
        @events.add("down", "#{node.name} went offline#{" (it was the leader)" if node == @last_leader}")
      elsif was_online == false && node.online?
        node.offline_since = nil
        @events.add("up", "#{node.name} is back online")
      end
      node.reported_online = node.online?
      return unless node.online?
      if !node.leader? && node.reported_in_isr == false && node.in_isr == true
        @events.add("sync", "#{node.name} has caught up and is in sync")
      end
      node.reported_in_isr = node.in_isr
    end

    private def detect_leader_change(now : Time) : Nil
      current = leader
      old = @last_leader
      if current.nil?
        if old && !@leaderless_since
          @leaderless_since = old.offline_since || now
          @events.add("election", "No leader, the remaining nodes are electing one")
        end
      elsif current != old
        @last_leader = current
        since = @leaderless_since
        @leaderless_since = nil
        term = [@term, current.term].compact.max?
        term_text = term ? " (term #{term})" : ""
        if since && old
          @events.add("leader", "#{current.name} took over as leader#{term_text}, " \
                                "#{(now - since).total_seconds.round(1)} s after #{old.name} stopped answering")
        else
          @events.add("leader", "#{current.name} is the leader#{term_text}")
        end
      end
    end

    def transfer_leadership : HTTP::Client::Response?
      host = leader.try(&.host)
      return unless host
      client = http_client(host, @config.http_port, 2.seconds)
      client.post("/api/cluster/transfer-leadership", headers: json_headers, body: "{}")
    rescue ex : IO::Error | Socket::Error
      Log.warn { "Transfer leadership failed: #{ex.message}" }
      nil
    ensure
      client.try &.close
    end

    # Raft addresses are host:port, IPv6 in brackets
    def self.address_host(address : String) : String
      address = address.lchop("tcp://")
      if address.starts_with?('[')
        address[1...address.index!(']')]
      else
        address.rpartition(':')[0].presence || address
      end
    end

    private def http_get(host, port, path, auth = true, timeout = 1.second) : String?
      response = http_get_response(host, port, path, auth, timeout)
      response.body if response && response.success?
    end

    private def http_get_response(host, port, path, auth = true, timeout = 1.second) : HTTP::Client::Response?
      client = http_client(host, port, timeout, auth)
      client.get(path)
    rescue IO::Error | Socket::Error
      nil
    ensure
      client.try &.close
    end

    private def http_client(host, port, timeout, auth = true) : HTTP::Client
      client = HTTP::Client.new(host, port)
      client.connect_timeout = timeout
      client.read_timeout = timeout
      client.write_timeout = timeout
      client.basic_auth(@config.user, @config.password) if auth
      client
    end

    private def json_headers
      HTTP::Headers{"Content-Type" => "application/json"}
    end
  end

  # Just enough of the Prometheus text format for unlabeled gauges
  module Prometheus
    def self.parse(text : String) : Hash(String, Float64)
      metrics = Hash(String, Float64).new
      text.each_line do |line|
        next if line.empty? || line.starts_with?('#')
        next if line.includes?('{')
        name, _, value = line.partition(' ')
        if v = value.split(' ', 2).first.to_f?
          metrics[name] = v
        end
      end
      metrics
    end
  end
end
