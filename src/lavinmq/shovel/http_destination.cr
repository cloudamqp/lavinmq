require "http/client"
require "openssl/hmac"
require "uuid"
require "base64"
require "./destination"

module LavinMQ
  module Shovel
    class HTTPDestination < Destination
      @client : ::HTTP::Client?
      @signature_keys : Array(Bytes)

      BASE64 = /\A(?:[A-Za-z0-9+\/]{4})*(?:[A-Za-z0-9+\/]{2}==|[A-Za-z0-9+\/]{3}=)?\z/

      # Multiple space separated secrets let receivers rotate keys without downtime
      def initialize(@name : String, @uri : URI, @ack_mode = DEFAULT_ACK_MODE, signature_secret : String? = nil)
        @signature_keys = self.class.decode_secrets(signature_secret || "")
      end

      # Standard Webhooks secrets are base64 of 24 to 64 random bytes, usually prefixed with whsec_
      def self.decode_secrets(value : String) : Array(Bytes)
        value.split(' ', remove_empty: true).map do |secret|
          encoded = secret.lchop("whsec_")
          key = Base64.decode(encoded) if BASE64.matches?(encoded)
          unless key && key.size.in?(24..64)
            raise ConfigError.new(
              "dest-signature-secret must be base64 of 24 to 64 bytes, optionally prefixed with whsec_")
          end
          key
        end
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
        response = if @signature_keys.empty?
                     c.post(path, headers: headers, body: msg.body_io)
                   else
                     body = msg.body_io.to_slice
                     add_signature_headers(headers, body, msg.properties.message_id)
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
      # A message-id keeps webhook-id stable across redeliveries, so receivers can dedupe
      private def add_signature_headers(headers, body : Bytes, message_id : String?)
        webhook_id = message_id || "msg_#{UUID.random.hexstring}"
        timestamp = (RoughTime.unix_ms // 1000).to_s
        headers["webhook-id"] = webhook_id
        headers["webhook-timestamp"] = timestamp
        headers["webhook-signature"] = @signature_keys.join(' ') do |key|
          self.class.signature(webhook_id, timestamp, body, key)
        end
      end

      def self.signature(webhook_id : String, timestamp : String, body : Bytes, key : Bytes) : String
        signed_content = IO::Memory.new(webhook_id.bytesize + timestamp.bytesize + body.size + 2)
        signed_content << webhook_id << '.' << timestamp << '.'
        signed_content.write(body)
        digest = OpenSSL::HMAC.digest(OpenSSL::Algorithm::SHA256, key, signed_content.to_slice)
        String.build(3 + 44) do |str|
          str << "v1,"
          Base64.strict_encode(digest, str)
        end
      end
    end
  end
end
