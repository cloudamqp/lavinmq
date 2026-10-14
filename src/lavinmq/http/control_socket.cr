require "http/server"
require "../logger"

module LavinMQ
  module HTTP
    # The control socket can't be bound
    class ControlSocketError < Exception; end

    class ControlSocketInUseError < ControlSocketError; end

    # The internal UNIX socket used by lavinmqctl. The Launcher binds it once
    # for the process and keeps it through the node's role changes: while the
    # node serves, requests go to the broker's HTTP API (#api=). Otherwise
    # they get a 503 saying it's a follower, or with *cluster_status*,
    # GET /api/cluster gets this node's view of the cluster, so it can be
    # inspected also when there's no leader to proxy to.
    class ControlSocket
      Log = LavinMQ::Log.for "http.control_socket"

      # Verifies that the control socket path is safe to bind to.
      # Deletes the file if it's a socket no one is listening on,
      # raises if it's in use, not a socket, or can't be verified.
      def self.prepare(path : String) : Nil
        return unless info = File.info?(path, follow_symlinks: false)

        unless info.type.socket?
          raise ControlSocketError.new("Control socket #{path} exists and is not a socket")
        end

        begin
          UNIXSocket.open(path) { }
          raise ControlSocketInUseError.new("Control socket #{path} is already in use")
        rescue Socket::ConnectError
          # ECONNREFUSED: socket inode exists, but nobody is listening.
          File.delete(path)
        rescue ex : Socket::Error
          # EACCES or anything ambiguous: fail closed, don't delete.
          raise ControlSocketError.new("Cannot verify stale control socket #{path}: #{ex.message}")
        end
      end

      getter path : String
      @api : ::HTTP::Handler? = nil
      @api_lock = Mutex.new
      @bound = false

      def initialize(@path : String, @cluster_status : Proc(String?)? = nil)
        @http = ::HTTP::Server.new { |context| handle(context) }
      end

      # Binds and listens, only for the owner and its group. Raises if it
      # can't, e.g. when another process on this machine serves the socket.
      def bind! : Nil
        return if @bound
        ControlSocket.prepare(@path)
        addr = @http.bind_unix(@path)
        begin
          File.chmod(@path, 0o660)
        rescue ex
          File.delete?(@path) # rather than left open to anyone
          raise ex
        end
        @bound = true
        Log.info { "Bound to #{addr}" }
        spawn(name: "Control socket listener") do
          @http.listen
        rescue ex
          raise ex unless @http.closed? # closed before listen started
        end
      end

      # Like #bind!, but false with a warning when it can't: a clustered node
      # may share the machine with another node serving the socket
      def bind : Bool
        bind!
        true
      rescue ex
        Log.warn { "#{ex.message}, not serving lavinmqctl socket on this node" }
        false
      end

      # Where requests go while this node serves, nil when it doesn't
      def api=(handler : ::HTTP::Handler?) : Nil
        @api_lock.synchronize { @api = handler }
      end

      def close : Nil
        @http.close unless @http.closed?
        File.delete?(@path) if @bound
        @bound = false
      end

      private def handle(context : ::HTTP::Server::Context) : Nil
        if api = @api_lock.synchronize { @api }
          return api.call(context)
        end
        if (status = @cluster_status) && context.request.method == "GET" &&
           context.request.path == "/api/cluster" && (json = status.call)
          context.response.content_type = "application/json"
          context.response.print json
          return
        end
        context.response.status_code = 503
        context.response.print "This node is a follower and does not handle lavinmqctl commands. \n" \
                               "Please connect to the leader node by using the --host option."
      end
    end
  end
end
