require "http/client"
require "json"

module LavinMQ
  class Etcd
    Log = ::Log.for(self)

    def initialize(endpoints = "127.0.0.1:2379")
      @endpoints = endpoints.split(',')
    end

    getter endpoints

    def get(key) : String?
      json = post("/v3/kv/range", %({"key":"#{Base64.strict_encode key}"}))
      if value = json.dig?("kvs", 0, "value").try(&.as_s)
        Base64.decode_string value
      end
    end

    def put(key, value) : String?
      body = %({"key":"#{Base64.strict_encode key}","value":"#{Base64.strict_encode value}","prev_kv":true})
      json = post("/v3/kv/put", body)
      if value = json.dig?("prev_kv", "value")
        Base64.decode_string value.as_s
      end
    end

    def del(key) : Int32
      json = post("/v3/kv/deleterange", %({"key":"#{Base64.strict_encode key}"}))
      json.dig?("deleted").try(&.as_s.to_i) || 0
    end

    def watch(key, &)
      body = %({"create_request":{"key":"#{Base64.strict_encode key}"}})
      post_stream("/v3/watch", body) do |json|
        next if json.dig?("result", "created") == true # "watch created" is first event

        if value = json.dig?("result", "events", 0, "kv", "value")
          yield Base64.decode_string(value.as_s)
        elsif json.dig?("result", "events", 0, "type").try(&.as_s) == "DELETE"
          yield nil
        else
          raise Error.new("Unexpected response: #{json}")
        end
      end
    end

    # Returns {ID, TTL}
    def lease_grant(ttl = 5) : Tuple(Int64, Int32)
      json = post("/v3/lease/grant", body: %({"TTL":"#{ttl}"}))
      ttl = json.dig("TTL").as_s.to_i
      id = json.dig("ID").as_s.to_i64
      {id, ttl}
    end

    def lease_keepalive(id) : Int32
      json = post("/v3/lease/keepalive", body: %({"ID":"#{id}"}))
      if ttl = json.dig?("result", "TTL")
        ttl.as_s.to_i
      else
        raise Error.new("Lost lease #{id}")
      end
    end

    def lease_revoke(id) : Nil
      post("/v3/kv/lease/revoke", body: %({"ID":"#{id}"}))
      true
    end

    # Leader election campaign
    # Returns the lease when the leadership is aquired
    def election_campaign(name, value, lease = 0i64) : Int64
      body = %({"name":"#{Base64.strict_encode name}", "value":"#{Base64.strict_encode value}","lease":"#{lease}"})
      json = post("/v3/election/campaign", body: body)
      json.dig("leader", "lease").as_s.to_i64
    end

    # Campaign for an election
    # Returns a Channel when elected leader,
    # when the channel is closed the leadership is lost
    def elect(name, value, ttl = 5) : Channel(Nil)
      channel = Channel(Nil).new
      lease_id, ttl = lease_grant(ttl)
      spawn(name: "Etcd lease keepalive #{lease_id}") do
        loop do
          select
          when channel.receive?
            lease_revoke(lease_id)
            channel.close
            break
          when timeout((ttl / 2.0).seconds)
            ttl = lease_keepalive(lease_id)
          end
        rescue ex
          Log.warn { "Lost leadership of #{name}: #{ex}" }
          channel.close
          break
        end
      end
      election_campaign(name, value, lease_id)
      channel
    end

    def elect_listen(name, &)
      post_stream("/v3/election/observe", %({"name":"#{Base64.strict_encode name}"})) do |json|
        if value = json.dig?("result", "kv", "value")
          yield Base64.decode_string(value.as_s)
        elsif error = json.dig?("error", "message")
          raise Error.new(error.as_s)
        else
          raise Error.new(json.to_s)
        end
      end
    end

    private def post(path, body) : JSON::Any
      with_tcp do |tcp, address|
        post_request(tcp, address, path, body)
      rescue ex : IO::Error | ResponseError
        Log.warn { "Failed request to #{address}#{path}: #{ex.message}" }
        tcp.close
        raise ex
      end
    end

    private def post_request(tcp, address, path, body)
      send_request(tcp, address, path, body)
      status, content_length = headers(tcp)
      json =
        case content_length
        when -1 # chunked response
          bytesize = tcp.read_line.to_i(16)
          chunk = tcp.read_string(bytesize)
          raise ResponseError.new unless tcp.read_line.empty?
          raise ResponseError.new unless tcp.read_line == "0"
          raise ResponseError.new unless tcp.read_line.empty?
          JSON.parse(chunk)
        when 0 # empty response body
        else
          body = tcp.read_string(content_length)
          JSON.parse(body)
        end
      raise StatusNotOk.new(status) unless status == "HTTP/1.1 200 OK"
      raise EmptyResponse.new if json.nil?
      json
    end

    private def post_stream(path, body, & : JSON::Any -> _)
      loop do
        response_finished = false
        with_tcp do |tcp, address|
          send_request(tcp, address, path, body)
          status = tcp.read_line
          raise StatusNotOk.new(status) unless status == "HTTP/1.1 200 OK"
          chunked_response = false
          until (line = tcp.read_line).empty?
            if line == "Transfer-Encoding: chunked"
              chunked_response = true
            end
          end
          raise ResponseError.new("Expected a chunked response") unless chunked_response
          loop do
            bytesize = tcp.read_line.to_i(16)
            if bytesize > 0
              chunk = tcp.read_string(bytesize + 2)
              yield JSON.parse(chunk)
            else
              tcp.skip(2) # \r\n follows each chunk
              response_finished = true
              break
            end
          end
        rescue ex : IO::Error
          Log.warn { "Disconnected while streaming from #{address}#{path}: #{ex.message}" }
          sleep 1
        ensure
          tcp.close unless response_finished # the server will otherwise keep sending chunks
        end
      end
    end

    private def send_request(tcp : IO, address : String, path : String, body : String)
      tcp << "POST " << path << " HTTP/1.1\r\n"
      tcp << "Host: " << address << "\r\n"
      tcp << "Content-Length: " << body.bytesize << "\r\n"
      tcp << "\r\n"
      tcp << body
      tcp.flush
    end

    private def headers(tcp) : Tuple(String, Int32)
      status_line = tcp.read_line
      content_length = -1
      until (line = tcp.read_line).empty?
        if line.starts_with? "Content-Length: "
          content_length = line.split(':', 2).last.to_i
        end
      end
      {status_line, content_length}
    end

    @connections = Deque(Tuple(TCPSocket, String)).new

    private def with_tcp(& : Tuple(TCPSocket, String) -> _)
      loop do
        socket, address = @connections.shift? || connect
        begin
          return yield({socket, address})
        rescue ex : IO::Error
          Log.warn { "Lost connection to #{address}, retrying" }
        ensure
          @connections.push({socket, address}) unless socket.closed?
        end
      end
    end

    private def connect : Tuple(TCPSocket, String)
      @endpoints.shuffle!.each do |address|
        host, port = address.split(':', 2)
        socket = TCPSocket.new(host, port, connect_timeout: 1.seconds)
        socket.sync = false
        socket.read_buffering = true
        socket.keepalive = true
        socket.tcp_keepalive_idle = 5
        socket.tcp_keepalive_count = 3
        socket.tcp_keepalive_interval = 1
        update_endpoints(socket, address)
        return {socket, address}
      rescue IO::Error
        next
      end
      raise Error.new("No endpoint responded")
    end

    private def update_endpoints(tcp, address)
      json = post_request(tcp, address, "/v3/cluster/member/list", "")
      endpoints = Array(String).new
      json["members"].as_a.each do |m|
        m["clientURLs"].as_a.each do |url|
          uri = URI.parse url.as_s
          if uri.scheme == "http" # Doesn't support https yet
            endpoints << "#{uri.hostname || "127.0.0.1"}:#{uri.port || 2379}"
          end
        end
      end
      unless @endpoints.size == endpoints.size &&
             @endpoints.all? { |addr| endpoints.includes? addr }
        Log.info { "Updated endpoints to: #{endpoints}" }
        @endpoints = endpoints
      end
    rescue ex : KeyError
      Log.warn { "Could not update endpoints, response was: #{json}" }
      raise ex
    end

    class Error < Exception; end

    class EmptyResponse < Error; end

    class StatusNotOk < Error; end

    class ResponseError < Error
      def initialize(msg = "Unexpected response")
        super
      end
    end
  end
end
