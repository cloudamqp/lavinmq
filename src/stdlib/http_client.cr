require "http/client"

# Backport of 1.0 feature https://github.com/crystal-lang/crystal/pull/9543
class HTTP::Client
  @io : IO?
  @reconnect = true

  # Creates a new HTTP client bound to an existing `IO`.
  # *host* and *port* can be specified and they will be used
  # to conform the `Host` header on each request.
  # Instances created with this constructor cannot be reconnected. Once
  # `close` is called explicitly or if the connection doesn't support keep-alive,
  # the next call to make a request will raise an exception.
  def initialize(@io : IO, @host = "", @port = 80)
    @reconnect = false
  end

  private def exec_internal_single(request)
    decompress = send_request(request)
    HTTP::Client::Response.from_io?(io, ignore_body: request.ignore_body?, decompress: decompress)
  end

  private def exec_internal_single(request)
    decompress = send_request(request)
    HTTP::Client::Response.from_io?(io, ignore_body: request.ignore_body?, decompress: decompress) do |response|
      yield response
    end
  end

  private def send_request(request)
    decompress = set_defaults request
    run_before_request_callbacks(request)
    request.to_io(io)
    io.flush
    decompress
  end

  def close
    @io.try &.close
    @io = nil
  end

  private def io
    io = @io
    return io if io
    unless @reconnect
      raise "This HTTP::Client cannot be reconnected"
    end

    hostname = @host.starts_with?('[') && @host.ends_with?(']') ? @host[1..-2] : @host
    io = TCPSocket.new hostname, @port, @dns_timeout, @connect_timeout
    io.read_timeout = @read_timeout if @read_timeout
    io.write_timeout = @write_timeout if @write_timeout
    io.sync = false

    {% if !flag?(:without_openssl) %}
      if tls = @tls
        tcp_socket = io
        begin
          io = OpenSSL::SSL::Socket::Client.new(tcp_socket, context: tls, sync_close: true, hostname: @host)
        rescue exc
          # don't leak the TCP socket when the SSL connection failed
          tcp_socket.close
          raise exc
        end
      end
    {% end %}

    @io = io
  end
end
