require "http/server/handler"

module LavinMQ
  module HTTP
    class ApiErrorHandler
      Log = LavinMQ::Log.for "http.apierror"
      include ::HTTP::Handler

      def call(context)
        call_next(context)
      rescue ex : JSON::Error | ArgumentError | TypeCastError
        L.error "Bad request", method: context.request.method, path: context.request.path, status: 400, exception: ex
        context.response.status_code = 400
        message = ex.message.to_s.split(", at /").first || "Unknown error"
        {error: "bad_request", reason: "#{message}"}.to_json(context.response)
      rescue ex : Controller::HaltRequest
        L.info "Halt request", method: context.request.method, path: context.request.path, status: context.response.status_code, message: ex.message
      rescue ex : IO::Error | ::HTTP::Server::ClientError
        L.info "Client/IO error", method: context.request.method, path: context.request.path, exception: ex
      rescue ex : LavinMQ::Exchange::AccessRefused
        L.error "Access refused", method: context.request.method, path: context.request.path, status: 403
        context.response.status_code = 403
        {error: "access_refused", reason: "Access Refused"}.to_json(context.response)
      rescue ex : Exception
        L.error "Unexpected error", method: context.request.method, path: context.request.path, status: 500, exception: ex
        context.response.status_code = 500
        {error: "internal_server_error", reason: "Internal Server Error"}.to_json(context.response)
      end
    end
  end
end
