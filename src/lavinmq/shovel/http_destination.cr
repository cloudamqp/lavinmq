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
        new(
          jitter: parameters["dest-jitter"]?.try &.as_f? || 1.0,
          backoff: parameters["dest-backoff"]?.try &.as_f? || 2.0,
          timeout: parameters["dest-timeout"]?.try &.as_f? || 30.0,
          max_retries: parameters["dest-max-retries"]?.try &.as_i? || 0
        )
      end
    end

    class HTTPDestination < Destination
      @client : ::HTTP::Client?

      def initialize(@name : String, @uri : URI, @parameters : HTTPDestinationParameters, @ack_mode = DEFAULT_ACK_MODE)
      end

      def start
        return if started?
        client = ::HTTP::Client.new @uri
        client.connect_timeout = 10.seconds
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
          @client.not_nil!.post(path, headers: headers, body: msg.body_io).success?
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
