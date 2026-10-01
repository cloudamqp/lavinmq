require "http/client"
require "json"
require "base64"
require "../logger"

module LavinMQ::Clustering
  # Read-only view of the etcd keys an etcd-coordinated cluster kept, for
  # migrating to the built-in raft election without stopping the cluster:
  # a node without raft state follows the etcd leader until its lease is
  # gone, then seeds raft with the final ISR from etcd. Never writes to etcd.
  class EtcdSeed
    Log = LavinMQ::Log.for "clustering.etcd_seed"

    class Error < Exception; end

    record Endpoint, uri : URI, auth : String?

    # The etcd election as of `revision`: the advertised clustering URI of
    # the node holding it, nil when nobody does.
    record Election, leader_uri : String?, revision : Int64

    @endpoints : Array(Endpoint)
    @watch_client : ::HTTP::Client? = nil
    @closed = false

    def initialize(endpoints : String, @prefix : String)
      @endpoints = endpoints.split(',', remove_empty: true).map { |e| parse_endpoint(e.strip) }
      raise ArgumentError.new("No etcd endpoints") if @endpoints.empty?
    end

    # Candidates in an etcd election each hold a key under the election
    # prefix, and the one created first is the leader. Unlike the election
    # API's leader call, this also says which revision the answer is from, so
    # a watch can continue from exactly there.
    def election : Election
      json = post("/v3/kv/range", {
        key:         Base64.strict_encode(election_prefix),
        range_end:   Base64.strict_encode(prefix_end(election_prefix)),
        sort_order:  "ASCEND",
        sort_target: "CREATE",
        limit:       1,
      }.to_json)
      revision = json.dig?("header", "revision").try(&.as_s.to_i64) || 0i64
      uri = json.dig?("kvs", 0, "value").try { |v| Base64.decode_string(v.as_s) }
      Election.new(uri, revision)
    end

    # Block until a candidate key is added or removed after `revision`, e.g.
    # the leader's lease expiring or another node taking over. Event driven:
    # a watch stream from `revision + 1`, so no change in between is missed.
    # Raises when the stream can't be set up or breaks; then read `election`
    # again and watch from its revision.
    def wait_for_election_change(revision : Int64) : Nil
      body = {create_request: {
        key:            Base64.strict_encode(election_prefix),
        range_end:      Base64.strict_encode(prefix_end(election_prefix)),
        start_revision: (revision + 1).to_s,
      }}.to_json
      each_endpoint do |ep|
        client = @watch_client = new_client(ep)
        # etcd only writes to the stream when something changes, so there is
        # no useful read timeout. #close interrupts it on stop.
        client.read_timeout = nil
        client.post("/v3/watch", headers: headers(ep), body: body) do |response|
          response.body_io.each_line do |line|
            json = parse(line)
            result = json["result"]? || json
            if result["canceled"]?.try(&.as_bool?)
              raise Error.new("Watch canceled: #{result["cancel_reason"]? || "compacted"}")
            end
            return if result["events"]?.try(&.as_a?).try(&.present?)
          end
        end
        raise IO::EOFError.new("Watch stream closed")
      ensure
        @watch_client = nil
        client.try &.close
      end
    end

    # Interrupts a blocked #wait_for_election_change.
    def close : Nil
      @closed = true
      @watch_client.try &.close
    end

    # The ISR the etcd leader last committed, nil on a cluster that never
    # recorded one. Same encoding as the etcd coordinator wrote: base36 ids,
    # comma separated.
    def isr : Set(Int32)?
      get("#{@prefix}/isr").try do |raw|
        set = Set(Int32).new
        raw.split(',', remove_empty: true) { |id| set << id.to_i(36) }
        set
      end
    end

    # The replication secret the etcd-era leader authenticates followers with.
    def clustering_secret : String?
      get("#{@prefix}/clustering_secret")
    end

    private def election_prefix : String
      "#{@prefix}/leader/"
    end

    # The key range end covering every key starting with `key`.
    private def prefix_end(key : String) : String
      bytes = key.to_slice.dup
      bytes[-1] += 1
      String.new(bytes)
    end

    private def get(key : String) : String?
      json = post("/v3/kv/range", %({"key":"#{Base64.strict_encode key}"}))
      json.dig?("kvs", 0, "value").try { |v| Base64.decode_string(v.as_s) }
    end

    private def post(path : String, body : String) : JSON::Any
      result = nil
      each_endpoint do |ep|
        client = new_client(ep)
        client.read_timeout = 5.seconds
        result = parse(client.post(path, headers: headers(ep), body: body).body)
      ensure
        client.try &.close
      end
      result || raise Error.new("No response from etcd")
    end

    # Yields endpoints until one works, connection errors move on to the next.
    private def each_endpoint(& : Endpoint ->) : Nil
      last_error = nil
      @endpoints.each do |ep|
        raise Error.new("Closed") if @closed
        begin
          yield ep
          return
        rescue ex : IO::Error | Socket::Error
          raise Error.new("Closed") if @closed
          last_error = ex
          Log.debug { "etcd endpoint #{ep.uri.host}:#{ep.uri.port} failed: #{ex.message}" }
        end
      end
      raise Error.new("No etcd endpoint reachable: #{last_error.try &.message}")
    end

    private def new_client(ep : Endpoint) : ::HTTP::Client
      client = ::HTTP::Client.new(ep.uri)
      client.connect_timeout = 2.seconds
      client
    end

    private def headers(ep : Endpoint) : ::HTTP::Headers
      headers = ::HTTP::Headers{"Content-Type" => "application/json"}
      ep.auth.try { |a| headers["Authorization"] = a }
      headers
    end

    private def parse(body : String) : JSON::Any
      json = JSON.parse(body)
      message = json.dig?("error", "message").try(&.as_s?) || json["error"]?.try(&.as_s?)
      if message.nil? && json["code"]?.try(&.as_i?).try(&.positive?)
        message = json["message"]?.try(&.as_s?)
      end
      raise Error.new(message) if message
      json
    rescue JSON::ParseException
      raise Error.new("Unexpected response from etcd: #{body[0, 96]}")
    end

    private def parse_endpoint(endpoint : String) : Endpoint
      uri = URI.parse(endpoint.includes?("://") ? endpoint : "http://#{endpoint}")
      uri.scheme = "http" unless uri.scheme == "https"
      uri.port ||= 2379
      auth = if (u = uri.user) && (p = uri.password)
               "Basic #{Base64.strict_encode("#{u}:#{p}")}"
             end
      uri.user = nil
      uri.password = nil
      Endpoint.new(uri, auth)
    end
  end
end
