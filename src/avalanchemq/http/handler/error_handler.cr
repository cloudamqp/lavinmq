require "http/server/handler"

module AvalancheMQ
  module HTTP
    class ApiErrorHandler
      include ::HTTP::Handler
      Log = ::Log.for(self)

      def call(context)
        call_next(context)
      rescue ex : Server::UnknownContentType
        context.response.content_type = "text/plain"
        context.response.status_code = 415
        context.response.print ex.message
      rescue ex : Server::PayloadTooLarge
        context.response.status_code = 413
        {error: "payload_too_large", reason: "Result set too large, use pagination"}.to_json(context.response)
      rescue ex : Server::NotFoundError
        Log.info { "method=#{context.request.method} path=#{context.request.path} status=#{context.response.status_code} message=\"#{ex.message}\"" }
        not_found(context, ex.message)
      rescue ex : JSON::Error | Server::ExpectedBodyError | ArgumentError | TypeCastError
        Log.error(exception: ex) { "method=#{context.request.method} path=#{context.request.path} status=400 error=#{ex}" }
        context.response.status_code = 400
        message = ex.message.to_s.split(", at /").first || "Unknown error"
        {error: "bad_request", reason: "#{message}"}.to_json(context.response)
      rescue ex : Controller::HaltRequest
        Log.info { "method=#{context.request.method} path=#{context.request.path} status=#{context.response.status_code} message=\"#{ex.message}\"" }
      rescue ex : IO::Error
        Log.info { "method=#{context.request.method} path=#{context.request.path} error=\"#{ex.message}\"" }
      rescue ex : Exception
        Log.error(exception: ex) { "method=#{context.request.method} path=#{context.request.path} status=500 error=#{ex.inspect}" }
        context.response.status_code = 500
        {error: "internal_server_error", reason: "Internal Server Error"}.to_json(context.response)
      end

      def not_found(context, message = nil)
        context.response.content_type = "text/plain"
        context.response.status_code = 404
        context.response.print "Not found\n"
        context.response.print message
      end
    end
  end
end
