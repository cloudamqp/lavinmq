require "http/client"
require "json"

module LavinMQ
  class Etcd
    Log = ::Log.for("etcd")

    def initialize(endpoints = "localhost:2379")
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
      post("/v3/lease/revoke", body: %({"ID":"#{id}"}))
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
          when timeout((ttl * 0.9).seconds)
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
        else
          raise Error.new(json.to_s)
        end
      end
    end

    private def post(path, body) : JSON::Any
      with_tcp do |tcp, address|
        return post_request(tcp, address, path, body)
      end
    end

    private def post_request(tcp, address, path, body) : JSON::Any
      send_request(tcp, address, path, body)
      content_length = read_headers(tcp)
      case content_length
      when -1 # chunked response, expect only one chunk
        chunks = read_chunks(tcp)
        parse_json! chunks
      else
        body = tcp.read_string(content_length)
        parse_json! body
      end
    end

    private def post_stream(path, body, & : JSON::Any -> _)
      with_tcp do |tcp, address|
        send_request(tcp, address, path, body)
        content_length = read_headers(tcp)
        if content_length == -1 # Chunked response
          read_chunks(tcp) do |chunk|
            yield parse_json! chunk
          end
        else
          body = tcp.read_string(content_length)
          yield parse_json! body
        end
      end
    end

    private def read_chunks(tcp, & : String -> _) : Nil
      response_finished = false
      loop do
        bytesize = tcp.read_line.to_i(16)
        chunk = tcp.read_string(bytesize)
        tcp.skip(2) # each chunk ends with \r\n
        break if bytesize.zero?
        yield chunk
      end
      response_finished = true
    ensure
      tcp.close unless response_finished # the server will otherwise keep sending chunks
    end

    private def read_chunks(tcp) : String
      String.build do |str|
        loop do
          bytesize = tcp.read_line.to_i(16)
          IO.copy(tcp, str, bytesize)
          tcp.skip(2) # each chunk ends with \r\n
          break if bytesize.zero?
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

    # Parse response headers, return Content-Length (-1 implies chunked response)
    private def read_headers(tcp) : Int32
      status_line = tcp.read_line
      content_length = 0
      until (line = tcp.read_line).empty?
        case line
        when "Transfer-Encoding: chunked" then content_length = -1
        when /^Content-Length: (\d+)$/    then content_length = $~[1].to_i
        end
      end

      case status_line
      when "HTTP/1.1 200 OK"
      else
        if content_length == -1
          chunks = read_chunks(tcp)
          parse_json! chunks # should raise
          raise Error.new(status_line)
        else
          body = tcp.read_string(content_length)
          parse_json! body # should raise
          raise Error.new(status_line)
        end
      end
      content_length
    end

    @connections = Deque(Tuple(TCPSocket, String)).new

    private def with_tcp(& : Tuple(TCPSocket, String) -> _)
      loop do
        socket, address = @connections.shift? || connect
        begin
          return yield({socket, address})
        rescue ex : NoLeader
          raise ex # don't retry when leader is missing
        rescue ex : Error
          Log.warn { "Service Unavailable at #{address}, #{ex.message}, retrying" }
          socket.close rescue nil
          sleep 0.1
        rescue IO::Error
          Log.warn { "Lost connection to #{address}, retrying" }
          socket.close rescue nil
          sleep 0.1
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
        # update_endpoints(socket, address)
        Log.debug { "Connected to #{address}" }
        return {socket, address}
      rescue ex : IO::Error
        Log.debug { "Could not connect to #{address}: #{ex}" }
        next
      end
      raise Error.new("No endpoint responded")
    end

    private def update_endpoints(tcp, address)
      json = post_request(tcp, address, "/v3/cluster/member/list", "")
      endpoints = Array(String).new
      json["members"].as_a.each do |m|
        m["clientURLs"]?.try &.as_a.each do |url|
          uri = URI.parse url.as_s
          if uri.scheme == "http" # Doesn't support https yet
            endpoints << "#{uri.hostname || "127.0.0.1"}:#{uri.port || 2379}"
          end
        end
      end
      unless @endpoints.size == endpoints.size &&
             @endpoints.all? { |addr| endpoints.includes? addr }
        Log.info { "Updated endpoints to: #{endpoints} (from: #{@endpoints})" }
        @endpoints = endpoints
      end
    rescue ex : KeyError
      Log.warn { "Could not update endpoints, response was: #{json}" }
      raise ex
    end

    # Parses JSON but raises if there's a error message
    private def parse_json!(str : String) : JSON::Any
      json = JSON.parse(str)
      raise_if_error(json)
      json
    end

    private def raise_if_error(json)
      if error = json["error"]?
        Log.debug { "etcd error: #{error}" }
        if errorh = error.as_h?
          error_msg = errorh["message"].as_s
          case error_msg
          when "error reading from server: EOF"
            raise IO::EOFError.new(error_msg)
          when "etcdserver: no leader"
            raise NoLeader.new(error_msg)
          else
            raise Error.new error_msg
          end
        else
          raise Error.new error.as_s
        end
      end
    end

    class Error < Exception; end

    class NoLeader < Error; end
  end
end
