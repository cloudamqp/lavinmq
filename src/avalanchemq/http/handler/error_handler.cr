module AvalancheMQ
  class ApiErrorHandler
    include HTTP::Handler

    def initialize(@log : Logger)
    end

    def call(context)
      call_next(context)
    rescue ex : HTTPServer::UnknownContentType
      context.response.content_type = "text/plain"
      context.response.status_code = 415
      context.response.print ex.message
    rescue ex : HTTPServer::NotFoundError
      not_found(context, ex.message)
    rescue ex : JSON::Error | ArgumentError
      @log.error "path=#{context.request.path} status=400 error=#{ex.inspect}"
      context.response.status_code = 400
      { error: "#{ex.message}" }.to_json(context.response)
    rescue ex : Exception
      @log.error "path=#{context.request.path} status=500\n#{ex.inspect_with_backtrace}"
      context.response.status_code = 500
      { error: "Internal Server Error" }.to_json(context.response)
    end

    def not_found(context, message = nil)
      context.response.content_type = "text/plain"
      context.response.status_code = 404
      context.response.print "Not found\n"
      context.response.print message
    end
  end
end
