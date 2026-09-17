require "http/server/handler"
require "json"
require "uuid"
require "./errors"
require "./request"
require "./actions"
require "./queue_url"
require "../auth/user_store"
require "../config"

module LavinMQ
  module SQS
    # HTTP handler implementing the AWS JSON 1.0 protocol for SQS: request id,
    # authentication, parsing into a `Request`, dispatch and error rendering.
    class Handler
      include ::HTTP::Handler
      Log = LavinMQ::Log.for "sqs.handler"

      JSON_CONTENT_TYPE  = "application/x-amz-json-1.0"
      QUERY_CONTENT_TYPE = "application/x-www-form-urlencoded"
      TARGET_PREFIX      = "AmazonSQS."
      MAX_BODY_SIZE      = 16 * 1024 * 1024
      DEFAULT_REGION     = "us-east-1"
      XML_NAMESPACE      = "http://queue.amazonaws.com/doc/2012-11-05/"

      def initialize(@users : Auth::UserStore, brokers : Brokers, @config : Config)
        @actions = Actions.new(brokers)
      end

      def call(context) : Nil
        request_id = UUID.random.to_s
        context.response.headers["x-amzn-RequestId"] = request_id
        begin
          handle(context, request_id)
        rescue ex : Error
          render_error(context, ex, request_id)
        rescue ex : JSON::ParseException
          render_error(context, SerializationException.new("Malformed JSON: #{ex.message}"), request_id)
        rescue ex : IO::Error | ::HTTP::Server::ClientError
          Log.info { "request_id=#{request_id} error=#{ex.inspect}" }
        rescue ex
          Log.error(exception: ex) { "request_id=#{request_id} action failed" }
          render_error(context, InternalError.new, request_id)
        end
      end

      private def handle(context, request_id : String) : Nil
        request = context.request
        unless request.method == "POST"
          raise InvalidAction.new("The request method #{request.method} is not valid for this endpoint. Use POST.")
        end
        content_type = request.headers["Content-Type"]? || ""
        unless content_type.starts_with?(JSON_CONTENT_TYPE)
          if content_type.starts_with?(QUERY_CONTENT_TYPE)
            raise InvalidAction.new("The AWS Query protocol is not supported, use an SDK that speaks the JSON protocol (awsJson1_0).")
          end
          raise InvalidAction.new("Unsupported Content-Type #{content_type}, expected #{JSON_CONTENT_TYPE}.")
        end
        target = request.headers["X-Amz-Target"]? || raise InvalidAction.new("Missing X-Amz-Target header.")
        unless target.starts_with?(TARGET_PREFIX)
          raise InvalidAction.new("The target #{target} is not valid for this endpoint.")
        end
        action = target.lchop(TARGET_PREFIX)
        user, region = authenticate(request)
        params = parse_body(request)
        req = Request.new(action, params, user, base_url(context), QueueUrl.vhost_from_path(request.path), region, request_id)
        Log.debug { "request_id=#{request_id} user=#{user.name} action=#{action}" }
        body = String.build do |io|
          JSON.build(io) do |json|
            json.object { @actions.dispatch(req, json) }
          end
        end
        response = context.response
        response.status_code = 200
        response.content_type = JSON_CONTENT_TYPE
        response.print body
      end

      # The access key id of the SigV4 credential names the LavinMQ user. The
      # signature itself is not verified: LavinMQ only stores password hashes,
      # and SigV4 needs the plaintext secret to recompute the signature.
      private def authenticate(request) : Tuple(Auth::User, String)
        access_key_id, region = credential(request)
        raise MissingAuthenticationToken.new if access_key_id.nil? || access_key_id.empty?
        user = @users[access_key_id]?
        raise InvalidClientTokenId.new if user.nil? || Auth::UserStore.hidden?(access_key_id)
        {user, region || DEFAULT_REGION}
      end

      # Returns {access_key_id, region} from the Authorization header
      # (`Credential=<id>/<date>/<region>/sqs/aws4_request`) or, for presigned
      # requests, the X-Amz-Credential query parameter.
      private def credential(request) : Tuple(String?, String?)
        scope = nil
        if auth = request.headers["Authorization"]?
          if idx = auth.index("Credential=")
            scope = auth[idx + 11..].split(',', 2).first
          end
        end
        scope ||= request.query_params["X-Amz-Credential"]?
        return {nil, nil} unless scope
        parts = scope.split('/')
        {parts[0]?, parts[2]?}
      end

      private def parse_body(request) : JSON::Any
        body = request.body || return JSON::Any.new(Hash(String, JSON::Any).new)
        if (length = request.content_length) && length > MAX_BODY_SIZE
          raise InvalidParameterValue.new("Request body too large.")
        end
        content = body.gets_to_end
        return JSON::Any.new(Hash(String, JSON::Any).new) if content.blank?
        parsed = JSON.parse(content)
        raise SerializationException.new("Request body must be a JSON object.") unless parsed.as_h?
        parsed
      end

      private def base_url(context) : String
        public_url = @config.sqs_public_url
        return public_url.rstrip('/') unless public_url.empty?
        host = context.request.headers["Host"]? || context.request.local_address.to_s
        local_port = context.request.local_address.as?(Socket::IPAddress).try(&.port)
        scheme = (@config.sqss_port > 0 && local_port == @config.sqss_port) ? "https" : "http"
        "#{scheme}://#{host}"
      end

      private def render_error(context, ex : Error, request_id : String) : Nil
        response = context.response
        response.status_code = ex.status
        response.headers["x-amzn-ErrorType"] = ex.code
        response.headers["x-amzn-query-error"] = "#{ex.query_code};#{ex.sender? ? "Sender" : "Receiver"}"
        if context.request.headers["Content-Type"]?.try(&.starts_with?(QUERY_CONTENT_TYPE))
          render_query_error(response, ex, request_id)
        else
          response.content_type = JSON_CONTENT_TYPE
          {"__type": "com.amazonaws.sqs##{ex.code}", "message": ex.message}.to_json(response)
        end
        Log.info { "request_id=#{request_id} status=#{ex.status} code=#{ex.code} message=#{ex.message.inspect}" } unless ex.status == 500
      rescue IO::Error
      end

      # Query protocol clients only get this one error (unsupported protocol),
      # but it has to be in their format to be shown as a readable exception.
      private def render_query_error(response, ex : Error, request_id : String) : Nil
        response.content_type = "text/xml"
        response << %(<?xml version="1.0"?><ErrorResponse xmlns="#{XML_NAMESPACE}"><Error><Type>)
        response << (ex.sender? ? "Sender" : "Receiver")
        response << "</Type><Code>"
        HTML.escape(ex.query_code, response)
        response << "</Code><Message>"
        HTML.escape(ex.message.to_s, response)
        response << "</Message></Error><RequestId>#{request_id}</RequestId></ErrorResponse>"
      end
    end
  end
end
