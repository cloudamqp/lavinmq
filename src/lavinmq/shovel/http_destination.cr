require "http/client"
require "./destination"
require "./retrier"

module LavinMQ
  module Shovel
    struct HTTPDestinationParameters
      getter jitter : Float64
      getter backoff : Float64
      getter timeout : Float64
      getter max_retries : Int32

      def initialize(@jitter, @backoff, @timeout, @max_retries)
      end

      def self.from_parameters(parameters : JSON::Any)
        jitter = Math.max(0.0, as_float(parameters["dest-jitter"]?) || 1.0)
        backoff = Math.max(0.0, as_float(parameters["dest-backoff"]?) || 2.0)
        timeout = Math.max(0.0, as_float(parameters["dest-timeout"]?) || 30.0)
        max_retries = Math.max(0, as_int(parameters["dest-max-retries"]?) || 0)
        new(jitter: jitter, backoff: backoff, timeout: timeout, max_retries: max_retries)
      end

      # Accept both JSON integer and float forms (e.g. `2` and `2.0`).
      private def self.as_float(value : JSON::Any?) : Float64?
        return nil unless value
        value.as_f? || value.as_i?.try(&.to_f)
      end

      private def self.as_int(value : JSON::Any?) : Int32?
        return nil unless value
        value.as_i? || value.as_f?.try(&.to_i)
      end
    end

    class HTTPDestination < Destination
      Log = LavinMQ::Log.for "shovel.http_destination"

      @client : ::HTTP::Client?

      def initialize(@name : String, @uri : URI, @parameters : HTTPDestinationParameters, @ack_mode = DEFAULT_ACK_MODE)
      end

      def start
        return if started?
        client = ::HTTP::Client.new @uri
        client.connect_timeout = @parameters.timeout.seconds
        client.read_timeout = @parameters.timeout.seconds
        client.basic_auth(@uri.user, @uri.password || "") if @uri.user
        @client = client
      end

      def stop
        @client.try &.close
      end

      def started? : Bool
        !@client.nil?
      end

      def push(msg, source)
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
        success = push_and_maybe_retry(@ack_mode) do
          msg.body_io.rewind
          c.post(path, headers: headers, body: msg.body_io).success?
        rescue ex : IO::Error | OpenSSL::SSL::Error
          # Timeouts, connection resets/refused etc. count as a failed attempt
          # so the retry loop applies; close the client to force a clean
          # reconnect on the next attempt.
          Log.warn { "shovel=#{@name} HTTP delivery failed: #{ex.message}" }
          c.close
          false
        end
        case @ack_mode
        in AckMode::OnConfirm, AckMode::OnPublish
          raise FailedDeliveryError.new unless success
          source.ack(msg.delivery_tag)
        in AckMode::NoAck
        end
      end

      private def push_and_maybe_retry(ack_mode, &push : -> Bool)
        return Retrier.push_with_retry(
          @parameters.max_retries,
          @parameters.jitter,
          @parameters.backoff,
          &push
        ) unless ack_mode == AckMode::NoAck
        push.call
      end
    end
  end
end
