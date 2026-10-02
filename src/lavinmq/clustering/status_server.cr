require "socket"
require "./leader_status"
require "../unix_socket"

module LavinMQ::Clustering
  # Streams LeaderStatus snapshots, one `key=value` line per change, to local
  # agents over a unix socket. The first line is the current state. EOF means
  # the state is unknown, e.g. lavinmq crashed.
  class StatusServer
    Log = LavinMQ::Log.for "clustering.status"

    WRITE_TIMEOUT = 5.seconds

    @server : UNIXServer? = nil

    def initialize(@status : LeaderStatus, @path : String)
    end

    def bind : Nil
      UnixSocket.prepare(@path)
      @server = UNIXServer.new(@path)
      File.chmod(@path, 0o660)
      Log.info { "Bound to #{@path}" }
    end

    def listen : Nil
      server = @server || raise "Not bound"
      while client = server.accept?
        spawn handle(client), name: "Clustering status client"
      end
    end

    def close : Nil
      @status.close
      if server = @server
        server.close
        File.delete?(@path)
      end
    end

    private def handle(client : UNIXSocket) : Nil
      client.sync = false
      client.read_buffering = false
      client.write_timeout = WRITE_TIMEOUT
      ch = @status.subscribe
      spawn(detect_close(client, ch), name: "Clustering status client reader")
      while snapshot = ch.receive?
        client << snapshot << '\n'
        client.flush
      end
    rescue IO::Error
    ensure
      @status.unsubscribe(ch) if ch
      client.close
    end

    private def detect_close(client : UNIXSocket, ch : Channel(LeaderStatus::Snapshot)) : Nil
      buf = uninitialized UInt8[64]
      while client.read(buf.to_slice) > 0
      end
    rescue IO::Error
    ensure
      @status.unsubscribe(ch)
    end
  end
end
