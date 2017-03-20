require "socket"
require "./cloudamqp-proxy/*"

class Proxy
  START_FRAME = UInt8.slice(65, 77, 81, 80, 0, 0, 9, 1)

  def handle_connection(socket)
    slice = Bytes.new(4096)
    bytes = socket.read(slice)

    if slice[0, bytes] != START_FRAME
      socket.write(START_FRAME)
      socket.close
      return
    end

    TCPSocket.open("localhost", 5672) do |remote|
      remote.write slice[0, bytes]
      loop do
        ready = IO.select([socket, remote], nil, nil)
        puts ready

        ready.each do |s|
          case s
          when socket
            bytes = socket.read(slice)
            return if bytes == 0
            puts slice[0, bytes].hexstring
            remote.write slice[0, bytes]
          when remote
            bytes = remote.read(slice)
            return if bytes == 0
            puts slice[0, bytes].hexstring
            socket.write slice[0, bytes]
          end
        end
      end
    end
  rescue ex : Errno
    puts ex
  ensure
    socket.close
    puts "conn closed"
  end

  def start
    server = TCPServer.new("localhost", 1234)
    loop do
      if socket = server.accept?
        puts "Accepted conn"
        # handle the client in a fiber
        spawn handle_connection(socket)
      else
        # another fiber closed the server
        break
      end
    end
  end
end

Proxy.new.start
