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

    @endpoints : Array(Endpoint)

    def initialize(endpoints : String, @prefix : String)
      @endpoints = endpoints.split(',', remove_empty: true).map { |e| parse_endpoint(e.strip) }
      raise ArgumentError.new("No etcd endpoints") if @endpoints.empty?
    end

    # Advertised clustering URI of the etcd election leader, nil when no node
    # holds the election (its lease expired or was released).
    def leader_uri : String?
      json = post("/v3/election/leader", %({"name":"#{Base64.strict_encode "#{@prefix}/leader"}"}))
      json.dig?("kv", "value").try { |v| Base64.decode_string(v.as_s) }
    rescue ex : Error
      return if ex.message.try &.includes?("election: no leader")
      raise ex
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

    private def get(key : String) : String?
      json = post("/v3/kv/range", %({"key":"#{Base64.strict_encode key}"}))
      json.dig?("kvs", 0, "value").try { |v| Base64.decode_string(v.as_s) }
    end

    private def post(path : String, body : String) : JSON::Any
      last_error = nil
      @endpoints.each do |ep|
        client = ::HTTP::Client.new(ep.uri)
        client.connect_timeout = 2.seconds
        client.read_timeout = 5.seconds
        headers = ::HTTP::Headers{"Content-Type" => "application/json"}
        ep.auth.try { |a| headers["Authorization"] = a }
        response = client.post(path, headers: headers, body: body)
        return parse(response.body)
      rescue ex : IO::Error | Socket::Error
        last_error = ex
        Log.debug { "etcd endpoint #{ep.uri.host}:#{ep.uri.port} failed: #{ex.message}" }
      ensure
        client.try &.close
      end
      raise Error.new("No etcd endpoint reachable: #{last_error.try &.message}")
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
