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
      @reused = false

      BASE64 = /\A(?:[A-Za-z0-9+\/]{4})*(?:[A-Za-z0-9+\/]{2}==|[A-Za-z0-9+\/]{3}=)?\z/

      Log = LavinMQ::Log.for "shovel.http_destination"

      getter timeout : Time::Span

      # Multiple space separated secrets let receivers rotate keys without downtime
      def initialize(@name : String, @uri : URI, @ack_mode = DEFAULT_ACK_MODE, @timeout : Time::Span = 30.seconds, signature_secret : String? = nil)
        @signature_keys = self.class.decode_secrets(signature_secret || "")
      end

      # Parses the `dest-timeout` shovel parameter (seconds, int or float) into a
      # connect/read timeout, falling back to 30s when absent or non-positive.
      def self.timeout_from(config : JSON::Any) : Time::Span
        secs = config["dest-timeout"]?.try { |v| v.as_f? || v.as_i?.try(&.to_f) }
        (secs && secs > 0 ? secs : 30.0).seconds
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

      def push(msg) : Nil
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
        add_signature_headers(headers, body, msg.properties.message_id) unless @signature_keys.empty?
        case @ack_mode
        in AckMode::OnConfirm, AckMode::OnPublish
          # The response is always awaited, so there is no earlier "published"
          # moment for HTTP: both modes classify the status. Acking on a failed
          # POST would silently lose the message.
          @listener.report(msg.delivery_tag, attempt(c, path, headers, body))
        in AckMode::NoAck
          attempt(c, path, headers, body) # nothing to settle: the outcome is dropped
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
        ::HTTP::Status::BAD_REQUEST,
        ::HTTP::Status::LENGTH_REQUIRED,
        ::HTTP::Status::PAYLOAD_TOO_LARGE,
        ::HTTP::Status::URI_TOO_LONG,
        ::HTTP::Status::UNSUPPORTED_MEDIA_TYPE,
        ::HTTP::Status::UNPROCESSABLE_ENTITY,
        ::HTTP::Status::REQUEST_HEADER_FIELDS_TOO_LARGE,
      }

      # Statuses that say "not right now" rather than "not this message".
      TRANSIENT_STATUSES = {
        ::HTTP::Status::REQUEST_TIMEOUT,
        ::HTTP::Status::TOO_MANY_REQUESTS,
      }

      def classify(response : ::HTTP::Client::Response) : Outcome
        status = response.status
        case
        when status.success?                      then Outcome::Confirmed
        when status.server_error?                 then Outcome::Retry
        when TRANSIENT_STATUSES.includes?(status) then Outcome::Retry
        when MESSAGE_STATUSES.includes?(status)   then Outcome::Reject
        else                                           Outcome::Abort
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
