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

      # Fast in-place retry for transient (Retry) delivery failures in OnConfirm
      # mode: re-POST a few times with a small random jitter (no backoff) so a
      # brief blip recovers without a broker round-trip. Exhausting the budget
      # reports Retry, handing the message back to the Runner to requeue (where
      # its own capped backoff throttles further attempts).
      MAX_RETRIES = 5
      JITTER      = 200.milliseconds

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
          outcome = deliver_with_outcome(headers, path, msg.body_io)
          @listener.report(msg.delivery_tag, outcome)
        in AckMode::OnPublish
          begin
            c.post(path, headers: headers, body: msg.body_io)
            @listener.report(msg.delivery_tag, Outcome::Confirmed)
          rescue IO::Error | OpenSSL::SSL::Error
            @listener.report(msg.delivery_tag, Outcome::Retry)
          end
        in AckMode::NoAck
          begin
            c.post(path, headers: headers, body: msg.body_io)
          rescue IO::Error | OpenSSL::SSL::Error
            # no_ack mode just ignore
          end
        end
      end

      # Delivers the message, retrying transient failures in place up to
      # MAX_RETRIES with a random jitter (no backoff). Re-classifies every
      # attempt, so a non-transient response (Reject/Abort) or a success returns
      # immediately; only Retry outcomes are retried.
      private def deliver_with_outcome(headers, path, body_io) : Outcome
        attempts = 0
        loop do
          outcome = attempt(headers, path, body_io)
          return outcome unless outcome.retry?
          return outcome if attempts >= MAX_RETRIES
          attempts += 1
          sleep JITTER * Random.rand(0.0..1.0)
        end
      end

      # A single delivery attempt. A transport-level failure (timeout, reset,
      # connection refused) closes and reopens the client so the next attempt
      # starts clean, and counts as a transient Retry.
      private def attempt(headers, path, body_io) : Outcome
        start unless started?
        body_io.rewind
        c = @client.not_nil!
        resp = c.post(path, headers: headers, body: body_io)
        classify resp
      rescue ex : IO::Error | OpenSSL::SSL::Error
        Log.warn { "shovel=#{@name} HTTP delivery failed: #{ex.message}" }
        @client.try &.close
        @client = nil
        # Heloo
        Outcome::Retry
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
