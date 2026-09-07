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
      end

      def stop
        @client.try &.close
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
        case @ack_mode
        in AckMode::OnConfirm
          @listener.report(msg.delivery_tag, attempt(c, path, headers, msg.body_io))
        in AckMode::OnPublish
          begin
            post(c, path, headers, msg.body_io)
            @listener.report(msg.delivery_tag, Outcome::Confirmed)
          rescue IO::Error | OpenSSL::SSL::Error
            @listener.report(msg.delivery_tag, Outcome::Retry)
          end
        in AckMode::NoAck
          begin
            post(c, path, headers, msg.body_io)
          rescue IO::Error | OpenSSL::SSL::Error
            # nothing to settle in no-ack mode
          end
        end
      end

      # A single delivery attempt, classified into an Outcome. A transport-level
      # failure counts as a transient Retry.
      private def attempt(c, path, headers, body_io) : Outcome
        classify post(c, path, headers, body_io)
      rescue IO::Error | OpenSSL::SSL::Error
        Outcome::Retry
      end

      # POST the message body. On a transport failure (timeout, reset,
      # connection refused, TLS error) the client is closed before re-raising:
      # HTTP::Client never drops a dead keep-alive socket by itself for a POST
      # with a body, and closing makes the next request open a fresh connection.
      private def post(c, path, headers, body_io) : ::HTTP::Client::Response
        body_io.rewind
        c.post(path, headers: headers, body: body_io)
      rescue ex : IO::Error | OpenSSL::SSL::Error
        Log.warn { "shovel=#{@name} HTTP delivery failed: #{ex.message}" }
        c.close
        raise ex
      end

      def classify(response : ::HTTP::Client::Response) : Outcome
        code = response.status_code
        case
        when 200 <= code < 300 then Outcome::Confirmed
        when code == 408       then Outcome::Retry
        when code == 429       then Outcome::Retry
        when 500 <= code < 600 then Outcome::Retry
        when code == 400       then Outcome::Reject
        when code == 422       then Outcome::Reject
        else                        Outcome::Abort
        end
      end
    end
  end
end
