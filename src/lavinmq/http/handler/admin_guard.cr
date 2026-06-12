require "http/server/handler"
require "json"

module LavinMQ
  module HTTP
    # Requires the administrator tag for every request under the given path
    # prefix. Mount in front of any handler whose routes mutate server or
    # cluster state but live outside the Controller hierarchy (which has
    # refuse_unless_administrator for the same job) — e.g. raft.cr's admin
    # handler. AuthHandler has resolved `context.user` and RequireUserHandler
    # has already rejected anonymous requests by the time this runs; this
    # enforces the tag, so an authenticated-but-unprivileged user (e.g.
    # monitoring) must not pass.
    class AdminGuard
      include ::HTTP::Handler

      def initialize(@path_prefix : String)
      end

      def call(context : ::HTTP::Server::Context)
        if context.request.path.starts_with?(@path_prefix)
          unless context.user.try &.tags.any?(&.administrator?)
            context.response.status_code = 403
            context.response.content_type = "application/json"
            {error: "access_refused", reason: "Administrator access required"}.to_json(context.response)
            return
          end
        end
        call_next(context)
      end
    end
  end
end
