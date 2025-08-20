require "socket"
require "wait_group"
require "json"
require "openssl"
require "./logging"
require "./etcd/lease"

module LavinMQ
  class Etcd
    Log = LavinMQ::Log.for "etcd"

    @endpoints : Array(Tuple(String, Int32, String?, Bool))

    def initialize(endpoints = "localhost:2379")
      @endpoints = endpoints.split(',').map { |ep| parse_endpoint(ep) }
    end

    private def parse_endpoint(endpoint) : Tuple(String, Int32, String?, Bool)
      uri = endpoint.includes?("://") ? URI.parse(endpoint) : URI.parse("tcp://#{endpoint}")
      auth = uri.user && uri.password ? "Basic #{Base64.strict_encode("#{uri.user}:#{uri.password}")}" : nil
      {uri.host || "localhost", uri.port || 2379, auth, uri.scheme == "https"}
    end

    def endpoints
      @endpoints.map { |host, port, _, _| "#{host}:#{port}" }
    end

    # Sets value if key doesn't exist
    # Return the value that was set or the value that was already stored
    def put_or_get(key, value) : String
      compare = %({"target":"CREATE","key":"#{Base64.strict_encode key}","create_revision":"0"})
      put = %({"requestPut":{"key":"#{Base64.strict_encode key}","value":"#{Base64.strict_encode value}"}})
      get = %{{"requestRange":{"key":"#{Base64.strict_encode key}"}}}
      request = %({"compare":[#{compare}],"success":[#{put}],"failure":[#{get}]})
      json = post("/v3/kv/txn", request)

      if stored_value = json.dig?("responses", 0, "response_range", "kvs", 0, "value").try &.as_s
        return Base64.decode_string stored_value
      elsif json.dig("responses", 0, "response_put")
        return value
      end

      raise Error.new("key #{key} not set")
    end

    def get(key) : String?
      json = post("/v3/kv/range", %({"key":"#{Base64.strict_encode key}"}))
      if value = json.dig?("kvs", 0, "value").try(&.as_s)
        Base64.decode_string value
      end
    end

    def get_prefix(key) : Hash(String, String)
      range_end = key.to_slice.dup
      range_end.update(-1) { |v| v + 1 }
      json = post("/v3/kv/range", %({"key":"#{Base64.strict_encode key}","range_end":"#{Base64.strict_encode range_end}"}))
      kvs = json["kvs"].as_a
      h = Hash(String, String).new(initial_capacity: kvs.size)
      kvs.each do |kv|
        key = Base64.decode_string kv["key"].as_s
        value = Base64.decode_string kv["value"].as_s
        h[key] = value
      end
      h
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
      stream("/v3/watch", body) do |json|
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
    def lease_grant(ttl = 10, id = 0) : Lease
      begin
        json = post("/v3/lease/grant", body: %({"TTL":"#{ttl}","ID":#{id}}))
      rescue LeaseAlreadyExists
        expires_in = lease_ttl(id) + 1
        Log.warn { "Cluster ID #{id.to_s(36)} already leased, waits #{expires_in}s for it to expire" }
        sleep expires_in.seconds
        json = post("/v3/lease/grant", body: %({"TTL":"#{ttl}","ID":#{id}}))
      end
      ttl = json.dig("TTL").as_s.to_i
      id = json.dig("ID").as_s.to_i64
      Lease.new(self, id, ttl)
    end

    def lease_keepalive(id) : Int32
      json = post("/v3/lease/keepalive", body: %({"ID":"#{id}"}))
      if ttl = json.dig?("result", "TTL")
        ttl.as_s.to_i
      else
        raise Error.new("Lease #{id} expired")
      end
    end

    def lease_ttl(id) : Int32
      json = post("/v3/lease/timetolive", body: %({"ID":"#{id}"}))
      if ttl = json["TTL"]?
        ttl.as_s.to_i
      else
        raise Error.new("Lease #{id} expired")
      end
    end

    def lease_revoke(id) : Nil
      post("/v3/lease/revoke", body: %({"ID":"#{id}"}))
    end

    # Leader election campaign
    # Returns the lease when the leadership is aquired
    def election_campaign(name, value, lease = 0i64) : Int64
      body = %({"name":"#{Base64.strict_encode name}", "value":"#{Base64.strict_encode value}","lease":"#{lease}"})
      json = post("/v3/election/campaign", body: body)
      json.dig("leader", "lease").as_s.to_i64
    end

    # Campaign for an election
    # Returns when elected leader
    # Returns a `Lease` instance
    def elect(name, value, ttl = 10, id = 0) : Lease
      lease = lease_grant(ttl, id)
      election_campaign(name, value, lease.id)
      lease
    end

    def elect_listen(name, &)
      stream("/v3/election/observe", %({"name":"#{Base64.strict_encode name}"})) do |json|
        if kv = json.dig?("result", "kv")
          if value = kv.dig?("value")
            yield Base64.decode_string(value.as_s)
          else
            yield nil
          end
        else
          raise Error.new("Unexpected election response: '#{json}'")
        end
      end
    end

    private def post(path, body) : JSON::Any
      with_tcp do |tcp, address, auth|
        return post_request(tcp, address, auth, path, body)
      end
    end

    private def post_request(tcp, address, auth, path, body) : JSON::Any
      send_request(tcp, address, auth, path, body)
      content_length = read_headers(tcp)
      case content_length
      when -1 # chunked response, expect only one chunk
        chunks = read_chunks(tcp)
        parse_json! chunks
      else
        body = read_string(tcp, content_length)
        parse_json! body
      end
    end

    private def stream(path, body, & : JSON::Any -> _)
      with_tcp do |tcp, address, auth|
        post_stream(tcp, address, auth, path, body) do |chunk|
          yield chunk
        end
      end
    end

    private def post_stream(tcp, address, auth, path, body, & : JSON::Any -> _)
      send_request(tcp, address, auth, path, body)
      content_length = read_headers(tcp)
      if content_length == -1 # Chunked response
        read_chunks(tcp) do |chunk|
          yield parse_json! chunk
        end
      else
        body = read_string(tcp, content_length)
        yield parse_json! body
      end
    end

    private def read_chunks(tcp, & : String -> _) : Nil
      response_finished = false
      loop do
        bytesize = read_chunk_size(tcp)
        chunk = read_string(tcp, bytesize)
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
    rescue ex : IO::Error
      raise Error.new("Read chunked response error", cause: ex)
    end

    def read_string(tcp, content_length) : String
      tcp.read_string(content_length)
    rescue ex : IO::Error
      raise Error.new("Read response error", cause: ex)
    end

    def read_chunk_size(tcp) : Int32
      tcp.read_line.to_i(16)
    rescue ex : IO::Error
      raise Error.new("Read response error", cause: ex)
    end

    private def send_request(tcp : IO, address : String, auth : String?, path : String, body : String)
      tcp << "POST " << path << " HTTP/1.1\r\n"
      tcp << "Host: " << address << "\r\n"
      tcp << "Content-Length: " << body.bytesize << "\r\n"
      tcp << "Authorization: " << auth << "\r\n" if auth
      tcp << "\r\n"
      tcp << body
      tcp.flush
    rescue ex : IO::Error
      raise Error.new("Send request error", cause: ex)
    end

    # Parse response headers, return Content-Length (-1 implies chunked response)
    private def read_headers(tcp) : Int32
      status_line = tcp.read_line
      content_length = 0
      loop do
        case tcp.read_line
        when "Transfer-Encoding: chunked" then content_length = -1
        when /^Content-Length: (\d+)$/    then content_length = $~[1].to_i
        when ""                           then break
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
    rescue ex : IO::Error
      raise Error.new("Read response error", cause: ex)
    end

    @connections = Deque(Tuple(IO, String, String?)).new
    @lock = Mutex.new

    private def with_tcp(& : Tuple(IO, String, String?) -> _)
      loop do
        socket, address, auth = @lock.synchronize { @connections.shift? } || connect
        begin
          return yield({socket, address, auth})
        rescue ex : LeaseAlreadyExists
          raise ex # don't retry
        rescue ex : NoLeader
          raise ex # don't retry when leader is missing
        rescue ex : Error
          Log.warn { "Service Unavailable at #{address}, #{ex.message}, retrying" }
          socket.close rescue nil
          sleep 0.1.seconds
        ensure
          @lock.synchronize { @connections.push({socket, address, auth}) } unless socket.closed?
        end
      end
    end

    private def connect : Tuple(IO, String, String?)
      @endpoints.shuffle.each do |host, port, auth, tls|
        address = "#{host}:#{port}"
        tcp_socket = create_tcp_socket(host, port)
        socket = tls ? wrap_with_tls(tcp_socket, host) : tcp_socket
        # update_endpoints(socket, address, auth)
        Log.debug { "Connected to #{address}" }
        return {socket, address, auth}
      rescue ex : IO::Error
        Log.debug { "Could not connect to #{address}: #{ex}" }
        next
      end
      Log.fatal { "No etcd endpoint responded" }
      exit 5 # 5th character in the alphabet is E(etcd)
    end

    private def create_tcp_socket(host, port)
      socket = TCPSocket.new(host, port, connect_timeout: 1.seconds)
      socket.sync = false
      socket.read_buffering = true
      socket.keepalive = true
      socket.tcp_keepalive_idle = 5
      socket.tcp_keepalive_count = 3
      socket.tcp_keepalive_interval = 1
      socket
    end

    private def wrap_with_tls(tcp_socket, host)
      ssl_context = OpenSSL::SSL::Context::Client.new
      ssl_socket = OpenSSL::SSL::Socket::Client.new(tcp_socket, context: ssl_context, hostname: host)
      ssl_socket.sync = false
      ssl_socket
    end

    private def update_endpoints(tcp, address, auth)
      json = post_request(tcp, address, auth, "/v3/cluster/member/list", "")
      new_endpoints = [] of Tuple(String, Int32, String?, Bool)
      json["members"].as_a.each do |m|
        m["clientURLs"]?.try &.as_a.each do |url|
          uri = URI.parse url.as_s
          if uri.scheme == "http" || uri.scheme == "https"
            host = uri.hostname || "127.0.0.1"
            port = uri.port || 2379
            tls = uri.scheme == "https"
            new_endpoints << {host, port, nil, tls}
          end
        end
      end
      old_addresses = endpoints
      new_addresses = new_endpoints.map { |host, port, _, _| "#{host}:#{port}" }
      unless old_addresses.size == new_addresses.size &&
             old_addresses.all? { |addr| new_addresses.includes? addr }
        Log.info { "Updated endpoints to: #{new_addresses} (from: #{old_addresses})" }
        @endpoints = new_endpoints
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
    rescue JSON::ParseException
      part = str[0, 96]
      part += "..." if str.size > 96
      raise Error.new("Unexpected response from etcd endpoint: #{part}")
    end

    private def raise_if_error(json)
      if error = json["error"]?
        Log.debug { "etcd error: #{error}" }
        error_msg =
          if errorh = error.as_h?
            errorh["message"].as_s
          else
            error.as_s
          end
        case error_msg
        when "error reading from server: EOF"
          raise IO::EOFError.new(error_msg)
        when "etcdserver: no leader"
          raise NoLeader.new(error_msg)
        when "etcdserver: lease already exists"
          raise LeaseAlreadyExists.new
        else
          raise Error.new error_msg
        end
      end
    end

    class Error < Exception; end

    class NoLeader < Error; end

    class LeaseAlreadyExists < Error; end
  end
end
