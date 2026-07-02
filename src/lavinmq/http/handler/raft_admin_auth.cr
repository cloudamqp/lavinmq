require "http/server/handler"
require "crypto/subtle"
require "json"
require "../../raft/backend"

module LavinMQ
  module HTTP
    # Authenticates the mutating /raft/admin/* surface with the cluster's
    # shared replication secret instead of a management user: a joining node
    # already holds `.clustering_password` (it needs it to follow the leader),
    # so cluster formation never depends on the user database. The basic-auth
    # password is compared against the secret; the username is ignored.
    # Mounted together with the admin handler before the auth stack, which
    # would otherwise reject the request (the secret is not a user).
    # Requests outside the prefix pass through untouched.
    class RaftAdminAuth
      include ::HTTP::Handler

      def initialize(@path_prefix : String, @backend : LavinMQ::Raft::Backend)
      end

      def call(context : ::HTTP::Server::Context)
        return call_next(context) unless context.request.path.starts_with?(@path_prefix)
        secret = @backend.password?
        provided = basic_auth_password(context.request)
        if secret && provided && Crypto::Subtle.constant_time_compare(secret, provided)
          call_next(context)
        else
          context.response.status_code = 401
          context.response.headers["WWW-Authenticate"] = %(Basic realm="raft-admin")
          context.response.content_type = "application/json"
          {error: "access_refused", reason: "clustering password required"}.to_json(context.response)
        end
      end

      private def basic_auth_password(request) : String?
        auth = request.headers["Authorization"]? || return
        return unless auth.starts_with?("Basic ")
        Base64.decode_string(auth[6..]).split(':', 2)[1]?
      rescue Base64::Error
        nil
      end
    end
  end
end
