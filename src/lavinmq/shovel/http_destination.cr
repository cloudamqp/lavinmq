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
        case @ack_mode
        in AckMode::OnConfirm
          begin
            response = c.post(path, headers: headers, body: msg.body_io)
            code = response.status_code
            outcome = case
                      when 200 <= code < 300 then Outcome::Confirmed
                      when code == 408       then Outcome::Retry
                      when code == 429       then Outcome::Retry
                      when 500 <= code < 600 then Outcome::Retry
                      when code == 400       then Outcome::Reject
                      when code == 422       then Outcome::Reject
                      else                        Outcome::Abort
                      end
            @listener.report(msg.delivery_tag, outcome)
          rescue IO::Error | Socket::Error
            @listener.report(msg.delivery_tag, Outcome::Retry)
          end
        in AckMode::OnPublish
          begin
            c.post(path, headers: headers, body: msg.body_io)
            @listener.report(msg.delivery_tag, Outcome::Confirmed)
          rescue IO::Error | Socket::Error
            @listener.report(msg.delivery_tag, Outcome::Retry)
          end
        in AckMode::NoAck
          begin
            c.post(path, headers: headers, body: msg.body_io)
          rescue IO::Error | Socket::Error
            # no_ack mode just ignore
          end
        end
      end
    end
  end
end
