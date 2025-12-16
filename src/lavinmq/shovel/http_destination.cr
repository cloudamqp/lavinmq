require "./destination"

module LavinMQ
  module Shovel
    class HTTPDestination < Destination
      @client : ::HTTP::Client?
      @signature_secrets : Array(String)

      def initialize(@name : String, @uri : URI, @ack_mode = DEFAULT_ACK_MODE, signature_secret : String? = nil)
        # Support multiple space-delimited secrets for key rotation (Standard Webhooks spec)
        # Empty strings are filtered out
        @signature_secrets = (signature_secret || "").split.reject!(&.empty?)
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
        if @signature_secrets.empty?
          # Stream body directly when signature is not needed
          response = c.post(path, headers: headers, body: msg.body_io)
        else
          # Read body for HMAC computation (already in memory as IO::Memory)
          body = msg.body_io.to_slice
          add_signature_headers(headers, body)
          response = c.post(path, headers: headers, body: body)
        end
        case @ack_mode
        in AckMode::OnConfirm, AckMode::OnPublish
          raise FailedDeliveryError.new unless response.success?
          source.ack(msg.delivery_tag)
        in AckMode::NoAck
        end
      end

      # Generate a webhook ID in Standard Webhooks format: msg_<uuid without dashes>
      private def generate_webhook_id : String
        "msg_#{UUID.random.hexstring}"
      end

      # Generate Unix timestamp in seconds
      private def generate_timestamp : String
        (RoughTime.unix_ms // 1000).to_s
      end

      # Generate signature in Standard Webhooks format: v1,<base64>
      # Signs: "{webhook-id}.{timestamp}.{body}"
      private def generate_signature(webhook_id : String, timestamp : String, body : Bytes, secret : String) : String
        # Build signed content directly in buffer to avoid intermediate String allocation
        signed_content = IO::Memory.new(webhook_id.bytesize + 1 + timestamp.bytesize + 1 + body.size)
        signed_content << webhook_id << '.' << timestamp << '.'
        signed_content.write(body)
        digest = OpenSSL::HMAC.digest(OpenSSL::Algorithm::SHA256, secret, signed_content.to_slice)
        String.build(3 + 44) do |str|
          str << "v1,"
          Base64.strict_encode(digest, str)
        end
      end

      # Add Standard Webhooks signature headers
      # Supports multiple secrets for zero-downtime key rotation (space-delimited signatures)
      private def add_signature_headers(headers, body)
        webhook_id = generate_webhook_id
        timestamp = generate_timestamp
        # Generate a signature for each secret, space-delimited per Standard Webhooks spec
        signatures = @signature_secrets.join(" ") { |secret| generate_signature(webhook_id, timestamp, body, secret) }
        headers["webhook-id"] = webhook_id
        headers["webhook-timestamp"] = timestamp.to_s
        headers["webhook-signature"] = signatures
        headers
      end
    end
  end
end
