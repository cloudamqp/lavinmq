require "http/client"
require "./destination"

module LavinMQ
  module Shovel
    class HTTPDestination < Destination
      Log = LavinMQ::Log.for "shovel.http_destination"

      # Parses the `dest-timeout` shovel parameter (seconds, int or float) into a
      # connect/read timeout, falling back to 30s when absent or non-positive.
      def self.timeout_from(config : JSON::Any) : Time::Span
        secs = config["dest-timeout"]?.try { |v| v.as_f? || v.as_i?.try(&.to_f) }
        (secs && secs > 0 ? secs : 30.0).seconds
      end

      @client : ::HTTP::Client?
      # True once a request has completed on the client's current connection,
      # i.e. the next request reuses a kept-alive socket.
      @reused = false

      getter timeout : Time::Span

      def initialize(@name : String, @uri : URI, @ack_mode = DEFAULT_ACK_MODE, @timeout : Time::Span = 30.seconds)
      end

      def start
        return if started?
        client = ::HTTP::Client.new @uri
        client.connect_timeout = @timeout
        client.read_timeout = @timeout
        client.basic_auth(@uri.user, @uri.password || "") if @uri.user
        @client = client
        @reused = false
      end

      def stop
        @client.try &.close
        @client = nil
        @reused = false
      end

      def started? : Bool
        !@client.nil?
      end

      def push(msg)
        c = @client || raise "Not started"
        headers = ::HTTP::Headers{"User-Agent" => "LavinMQ"}
        headers["X-Shovel"] = @name
        msg.properties.content_type.try { |v| headers["Content-Type"] = v }
        msg.properties.message_id.try { |v| headers["X-Message-Id"] = v }
        msg.properties.headers.try do |hs|
          hs.each do |k, v|
            headers["X-#{k}"] = v.to_s
          end
        end
        path = case
               when !@uri.path.empty?
                 @uri.path
               when p = msg.properties.headers.try &.["uri_path"]?
                 p.to_s
               else
                 "/"
               end
        body = msg.body_io.to_slice
        case @ack_mode
        in AckMode::OnConfirm
          @listener.report(msg.delivery_tag, attempt(c, path, headers, body))
        in AckMode::OnPublish
          begin
            post(c, path, headers, body)
            @listener.report(msg.delivery_tag, Outcome::Confirmed)
          rescue IO::Error | OpenSSL::SSL::Error
            @listener.report(msg.delivery_tag, Outcome::Retry)
          end
        in AckMode::NoAck
          begin
            post(c, path, headers, body)
          rescue IO::Error | OpenSSL::SSL::Error
            # nothing to settle in no-ack mode
          end
        end
      end

      # A single delivery attempt, classified into an Outcome. A transport-level
      # failure counts as a transient Retry.
      private def attempt(c, path, headers, body : Bytes) : Outcome
        classify post(c, path, headers, body)
      rescue IO::Error | OpenSSL::SSL::Error
        Outcome::Retry
      end

      # POST the message body (as Bytes, so the request carries a Content-Length
      # rather than chunked encoding). On a transport failure (timeout, reset,
      # connection refused, TLS error) the client is closed before re-raising:
      # HTTP::Client never drops a dead keep-alive socket by itself for a POST
      # with a body, and closing makes the next request open a fresh connection.
      #
      # An endpoint that closed an idle keep-alive is only detected by the next
      # request dying on it. That one case — EOF or a reset on a connection that
      # already served a request — is retried once on a fresh connection before
      # the failure counts. @reused is false by then, so the recursion is
      # bounded to a single retry.
      private def post(c, path, headers, body : Bytes) : ::HTTP::Client::Response
        reused = @reused
        resp = c.post(path, headers: headers, body: body)
        @reused = true
        resp
      rescue ex : IO::Error | OpenSSL::SSL::Error
        c.close
        @reused = false
        if reused && stale_connection?(ex)
          Log.debug { "shovel=#{@name} stale keep-alive (#{ex.message}), retrying on a fresh connection" }
          post(c, path, headers, body)
        else
          Log.warn { "shovel=#{@name} HTTP delivery failed: #{ex.message}" }
          raise ex
        end
      end

      # EOF or a reset mid-request is how a server-side close of an idle
      # keep-alive surfaces; a timeout or a refused connection is not that.
      private def stale_connection?(ex : Exception) : Bool
        return true if ex.is_a?(IO::EOFError)
        ex.is_a?(IO::Error) && ex.os_error.in?(Errno::ECONNRESET, Errno::EPIPE)
      end

      # Statuses that describe the request rather than the endpoint. The body,
      # Content-Type, uri_path and headers all come from the message, so these
      # mean "this message is unacceptable" (Reject), not "the endpoint is
      # unusable" (Abort).
      MESSAGE_STATUSES = {
        400, # Bad Request
        411, # Length Required
        413, # Payload Too Large
        414, # URI Too Long
        415, # Unsupported Media Type
        422, # Unprocessable Content
        431, # Request Header Fields Too Large
      }

      def classify(response : ::HTTP::Client::Response) : Outcome
        code = response.status_code
        case
        when 200 <= code < 300                then Outcome::Confirmed
        when code == 408                      then Outcome::Retry
        when code == 429                      then Outcome::Retry
        when 500 <= code < 600                then Outcome::Retry
        when MESSAGE_STATUSES.includes?(code) then Outcome::Reject
        else                                       Outcome::Abort
        end
      end
    end
  end
end
