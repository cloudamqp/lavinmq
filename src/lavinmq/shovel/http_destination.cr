require "http/client"
require "openssl/hmac"
require "uuid"
require "base64"
require "./destination"

module LavinMQ
  module Shovel
    class HTTPDestination < Destination
      @client : ::HTTP::Client?
      @signature_secrets : Array(String)

      # Multiple space separated secrets let receivers rotate keys without downtime
      def initialize(@name : String, @uri : URI, @ack_mode = DEFAULT_ACK_MODE, signature_secret : String? = nil)
        @signature_secrets = (signature_secret || "").split(' ', remove_empty: true)
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
        response = if @signature_secrets.empty?
                     c.post(path, headers: headers, body: msg.body_io)
                   else
                     body = msg.body_io.to_slice
                     add_signature_headers(headers, body)
                     c.post(path, headers: headers, body: body)
                   end
        case @ack_mode
        in AckMode::OnConfirm, AckMode::OnPublish
          raise FailedDeliveryError.new unless response.success?
          source.ack(msg.delivery_tag)
        in AckMode::NoAck
        end
      end

      # https://www.standardwebhooks.com/
      private def add_signature_headers(headers, body : Bytes)
        webhook_id = "msg_#{UUID.random.hexstring}"
        timestamp = (RoughTime.unix_ms // 1000).to_s
        headers["webhook-id"] = webhook_id
        headers["webhook-timestamp"] = timestamp
        headers["webhook-signature"] = @signature_secrets.join(' ') do |secret|
          signature(webhook_id, timestamp, body, secret)
        end
      end

      private def signature(webhook_id : String, timestamp : String, body : Bytes, secret : String) : String
        signed_content = IO::Memory.new(webhook_id.bytesize + timestamp.bytesize + body.size + 2)
        signed_content << webhook_id << '.' << timestamp << '.'
        signed_content.write(body)
        digest = OpenSSL::HMAC.digest(OpenSSL::Algorithm::SHA256, secret, signed_content.to_slice)
        String.build(3 + 44) do |str|
          str << "v1,"
          Base64.strict_encode(digest, str)
        end
      end
    end
  end
end
