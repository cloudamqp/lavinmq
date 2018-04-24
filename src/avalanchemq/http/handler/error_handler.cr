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
      @log.error "path=#{context.request.path} error=#{ex.inspect}"
      context.response.status_code = 400
      { error: "#{ex.inspect}" }.to_json(context.response)
    end

    def not_found(context, message = nil)
      context.response.content_type = "text/plain"
      context.response.status_code = 404
      context.response.print "Not found\n"
      context.response.print message
    end
  end
end
