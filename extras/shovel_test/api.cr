require "http/client"
require "json"
require "base64"
require "uri"
require "./config"

module ShovelTest
  # The broker's management HTTP API, scoped to the configured vhost.
  class Api
    class Error < Exception; end

    record QueueStats, ready : Int64, unacked : Int64 do
      def total : Int64
        ready + unacked
      end
    end

    record ShovelStatus,
      state : String,
      error : String?,
      confirmed : Int64,
      retried : Int64,
      rejected : Int64,
      aborted : Int64 do
      def state?(name : String) : Bool
        state.compare(name, case_insensitive: true) == 0
      end
    end

    def initialize(@cfg : Config)
      @auth = "Basic #{Base64.strict_encode("#{@cfg.user}:#{@cfg.password}")}"
      @vhost = URI.encode_path_segment(@cfg.vhost)
    end

    def request(method : String, path : String, body : String? = nil) : {Int32, String}
      uri = URI.parse("#{@cfg.http_base}#{path}")
      client = HTTP::Client.new(uri)
      client.connect_timeout = 5.seconds
      client.read_timeout = 10.seconds
      headers = HTTP::Headers{"Authorization" => @auth, "Content-Type" => "application/json"}
      resp = client.exec(method, uri.request_target, headers, body)
      {resp.status_code, resp.body.to_s}
    ensure
      client.try &.close
    end

    # Raises unless the response is 2xx.
    def request!(method : String, path : String, body : String? = nil) : String
      code, resp = request(method, path, body)
      raise Error.new("#{method} #{path} -> #{code} #{resp}") unless 200 <= code < 300
      resp
    end

    def server_version : String?
      JSON.parse(request!("GET", "/api/overview"))["lavinmq_version"]?.try(&.as_s?)
    end

    # -------- vhost --------

    def vhost_exists? : Bool
      request("GET", "/api/vhosts/#{@vhost}")[0] == 200
    end

    def create_vhost : Nil
      request!("PUT", "/api/vhosts/#{@vhost}")
      user = URI.encode_path_segment(@cfg.user)
      request!("PUT", "/api/permissions/#{@vhost}/#{user}", {configure: ".*", write: ".*", read: ".*"}.to_json)
    end

    def delete_vhost : Nil
      request!("DELETE", "/api/vhosts/#{@vhost}")
    end

    # -------- queues --------

    def declare_queue(name : String, arguments : Hash) : Nil
      request!("PUT", "/api/queues/#{@vhost}/#{seg name}", {arguments: arguments}.to_json)
    end

    def delete_queue(name : String) : Nil
      request("DELETE", "/api/queues/#{@vhost}/#{seg name}")
    end

    def queue(name : String) : QueueStats
      json = JSON.parse(request!("GET", "/api/queues/#{@vhost}/#{seg name}"))
      QueueStats.new(json["messages_ready"].as_i64, json["messages_unacknowledged"].as_i64)
    end

    # -------- shovels --------

    def create_shovel(name : String, value) : Nil
      request!("PUT", "/api/parameters/shovel/#{@vhost}/#{seg name}", {value: value}.to_json)
    end

    def delete_shovel(name : String) : Nil
      request("DELETE", "/api/parameters/shovel/#{@vhost}/#{seg name}")
    end

    def shovel_parameter?(name : String) : Bool
      request("GET", "/api/parameters/shovel/#{@vhost}/#{seg name}")[0] == 200
    end

    # nil once the broker no longer has the shovel
    def shovel(name : String) : ShovelStatus?
      code, body = request("GET", "/api/shovels/#{@vhost}/#{seg name}")
      return if code == 404
      raise Error.new("GET shovel #{name} -> #{code} #{body}") unless code == 200
      json = JSON.parse(body)
      ShovelStatus.new(
        json["state"].as_s,
        json["error"]?.try(&.as_s?),
        counter(json, "confirmed"),
        counter(json, "retried"),
        counter(json, "rejected"),
        counter(json, "aborted"),
      )
    end

    def pause_shovel(name : String) : Nil
      request!("PUT", "/api/shovels/#{@vhost}/#{seg name}/pause")
    end

    def resume_shovel(name : String) : Nil
      request!("PUT", "/api/shovels/#{@vhost}/#{seg name}/resume")
    end

    # -------- diagnostics --------

    def queues_summary : String
      JSON.parse(request!("GET", "/api/queues/#{@vhost}")).as_a.join("\n") do |q|
        "  queue #{q["name"]}: ready=#{q["messages_ready"]} unacked=#{q["messages_unacknowledged"]}"
      end
    end

    def shovels_summary : String
      JSON.parse(request!("GET", "/api/shovels/#{@vhost}")).as_a.join("\n") do |s|
        "  shovel #{s["name"]}: #{s.to_json}"
      end
    end

    private def counter(json : JSON::Any, key : String) : Int64
      json[key]?.try(&.as_i64?) || 0_i64
    end

    private def seg(name : String) : String
      URI.encode_path_segment(name)
    end
  end
end
