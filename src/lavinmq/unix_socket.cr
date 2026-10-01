require "socket"

module LavinMQ
  module UnixSocket
    class InUseError < Exception; end

    # Verifies that a unix socket path is safe to bind to.
    # Deletes the file if it's a socket no one is listening on,
    # raises if it's in use, not a socket, or can't be verified.
    def self.prepare(path : String) : Nil
      return unless info = File.info?(path, follow_symlinks: false)

      unless info.type.socket?
        raise "Unix socket #{path} exists and is not a socket"
      end

      begin
        UNIXSocket.open(path) { }
        raise InUseError.new("Unix socket #{path} is already in use")
      rescue Socket::ConnectError
        # ECONNREFUSED: socket inode exists, but nobody is listening.
        File.delete(path)
      rescue ex : Socket::Error
        # EACCES or anything ambiguous: fail closed, don't delete.
        raise "Cannot verify stale unix socket #{path}: #{ex.message}"
      end
    end
  end
end
