require "http/server/handler"

module LavinMQ
  module HTTP
    class ApiErrorHandler
      include ::HTTP::Handler

      def call(context)
        call_next(context)
      rescue ex : Server::UnknownContentType
        context.response.content_type = "text/plain"
        context.response.status_code = 415
        context.response.print ex.message
      rescue ex : Server::NotFoundError
        Log.info { "method=#{context.request.method} path=#{context.request.path} status=#{context.response.status_code} message=\"#{ex.message}\"" }
        not_found(context, ex.message)
      rescue ex : JSON::Error | Server::ExpectedBodyError | ArgumentError | TypeCastError
        error = Log.level == ::Log::Severity::Debug ? ex.inspect_with_backtrace : "\"#{ex.message}\""
        Log.error { "method=#{context.request.method} path=#{context.request.path} status=400 error=#{error}" }
        context.response.status_code = 400
        message = ex.message.to_s.split(", at /").first || "Unknown error"
        {error: "bad_request", reason: "#{message}"}.to_json(context.response)
      rescue ex : Controller::HaltRequest
        Log.info { "method=#{context.request.method} path=#{context.request.path} status=#{context.response.status_code} message=\"#{ex.message}\"" }
      rescue ex : IO::Error | ::HTTP::Server::ClientError
        Log.info { "method=#{context.request.method} path=#{context.request.path} error=\"#{ex.inspect}\"" }
      rescue ex : Exception
        Log.error { "method=#{context.request.method} path=#{context.request.path} status=500 error=#{ex.inspect_with_backtrace}" }
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
