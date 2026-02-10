module LavinMQ
  module HTTP
    class RequireUserHandler
      include ::HTTP::Handler

      def call(context)
        if context.user.nil?
          context.response.status_code = 401
        else
          call_next(context)
        end
      end
    end
  end
end
