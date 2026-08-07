module LavinMQ::Raft
  # The address a node advertises in raft.cr's peer record: two endpoints
  # packed as "raft_host:raft_port,data_host:data_port". The raft endpoint is
  # dialed by the consensus transport; the data endpoint is where followers
  # connect to replicate. This type owns encode + decode of that format so the
  # three call sites (advertise, register_peer, follow-leader) share one tested
  # codec.
  struct PeerAddress
    getter raft_host : String
    getter raft_port : Int32
    getter data_host : String
    getter data_port : Int32

    def initialize(@raft_host : String, @raft_port : Int32, @data_host : String, @data_port : Int32)
    end

    # A node advertises the same host for both endpoints (its own address).
    def self.new(host : String, raft_port : Int32, data_port : Int32)
      new(host, raft_port, host, data_port)
    end

    # Parse the packed form. Returns nil if malformed.
    def self.parse?(address : String) : PeerAddress?
      raft_part, sep, data_part = address.partition(",")
      return nil if sep.empty?
      rhost, rport = split_host_port(raft_part) || return nil
      dhost, dport = split_host_port(data_part) || return nil
      new(rhost, rport, dhost, dport)
    end

    # Splits "host:port" on the LAST colon (so IPv6 literals like "[::1]:5680"
    # keep their bracketed host). Returns nil if malformed.
    private def self.split_host_port(s : String) : {String, Int32}?
      host, sep, port = s.rpartition(":")
      return nil if sep.empty? || host.empty? || port.empty?
      port_i = port.to_i? || return nil
      {host, port_i}
    end

    def to_s(io : IO) : Nil
      io << raft_host << ':' << raft_port << ',' << data_host << ':' << data_port
    end

    # The raft consensus transport endpoint, with IPv6 brackets stripped:
    # URI#host keeps them ("[::1]") but TCPSocket needs the bare host.
    def raft_endpoint : {String, Int32}
      {raft_host.lstrip('[').rstrip(']'), raft_port}
    end

    # The data-plane URI a follower connects to. Brackets are KEPT so an IPv6
    # literal stays a valid URI authority (the client strips them via #hostname).
    def data_uri : String
      "tcp://#{data_host}:#{data_port}"
    end
  end
end
