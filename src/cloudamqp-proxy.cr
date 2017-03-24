require "socket"
require "amqp"
require "./cloudamqp-proxy/*"

module Proxy
  extend self
  START_FRAME = UInt8.slice(65, 77, 81, 80, 0, 0, 9, 1)

  def copy(i, o)
    buf = uninitialized UInt8[4096]
    loop do
      bytes = i.read(buf.to_slice)
      return if bytes == 0

      AMQP.parse_frame buf.to_slice
      o.write buf.to_slice[0, bytes]
    end
  rescue ex : AMQP::InvalidFrameEnd
    puts ex
    #socket.write Slice[1, 0, 0]
  rescue ex : Errno
    puts ex
  ensure
    i.close
    o.close
    puts "conn closed"
  end

  def handle_connection(socket)
    start = Bytes.new(8)
    bytes = socket.read_fully(start)

    if bytes != 8 || start != START_FRAME
      socket.write(START_FRAME)
      socket.close
      return
    end

    remote = TCPSocket.new("localhost", 5672)
    remote.write START_FRAME
    spawn copy(remote, socket)
    spawn copy(socket, remote)
  rescue ex : Errno
    puts ex
    remote.close if remote
    socket.close
  end

  def start
    server = TCPServer.new("localhost", 1234)
    loop do
      puts "Waiting for connections"
      if socket = server.accept?
        puts "Accepted conn"
        # handle the client in a fiber
        spawn handle_connection(socket)
        puts "Spawned fiber"
      else
        # another fiber closed the server
        break
      end
    end
  end
end

Proxy.start
