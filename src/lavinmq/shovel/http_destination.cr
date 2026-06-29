require "http/client"
require "./destination"

module LavinMQ
  module Shovel
    class HTTPDestination < Destination
      @client : ::HTTP::Client?

      def initialize(@name : String, @uri : URI, @ack_mode = DEFAULT_ACK_MODE)
      end

      def start
        return if started?
        client = ::HTTP::Client.new @uri
        client.connect_timeout = 10.seconds
        client.read_timeout = 30.seconds
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
        outcome = begin
          classify c.post(path, headers: headers, body: msg.body_io)
        rescue IO::Error | Socket::Error
          # Transport-level failure (connection refused, timeout, reset): the
          # endpoint may recover, so treat it as transient.
          Outcome::Retry
        end
        case @ack_mode
        in AckMode::OnConfirm, AckMode::OnPublish
          @on_outcome.call(msg.delivery_tag, outcome)
        in AckMode::NoAck
        end
      end

      # Maps an HTTP response to a delivery disposition. Pure and broker-free.
      #   2xx                          -> Confirmed
      #   408, 429, 5xx                -> Retry   (transient, retry with backoff)
      #   400, 422                     -> Reject  (bad message, dead-letter it)
      #   any other non-2xx (e.g. 401, -> Abort   (endpoint unusable; error-out
      #     403, 404, 405, 410, …)                 the shovel past a threshold)
      def classify(response : ::HTTP::Client::Response) : Outcome
        code = response.status_code
        case
        when 200 <= code < 300                               then Outcome::Confirmed
        when code == 408 || code == 429 || 500 <= code < 600 then Outcome::Retry
        when code == 400 || code == 422                      then Outcome::Reject
        else                                                      Outcome::Abort
        end
      end
    end
  end
end
