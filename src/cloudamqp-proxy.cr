require "socket"
require "./cloudamqp-proxy/amqp"
require "./cloudamqp-proxy/version"

module Proxy
  extend self
  START_FRAME = UInt8.slice(65, 77, 81, 80, 0, 0, 9, 1)

  def copy(i, o)
    loop do
      frame = AMQP.parse_frame i
      #frame = AMQP::Frame.parse i
      #puts frame.to_slice
      o.write frame.to_slice
    end
#  rescue ex : AMQP::InvalidFrameEnd
#    puts ex
#    #socket.write Slice[1, 0, 0]
  rescue ex : IO::EOFError | Errno
    puts ex
  ensure
    i.close
    o.close
    puts "conn closed"
  end

  def handle_connection(socket)
    puts "socket#sync=#{socket.sync?} socket#send_buffer_size=#{socket.send_buffer_size}"
    negotiate_client(socket)

    remote = TCPSocket.new("localhost", 5672)
    puts "remote#sync=#{remote.sync?} remote#send_buffer_size=#{remote.send_buffer_size}"
    negotiate_server(remote)

    spawn copy(remote, socket)
    spawn copy(socket, remote)
  rescue ex : Errno
    puts "handle_connection #{ex}"
    remote.close if remote
    socket.close
  end

  def negotiate_server(remote)
    remote.write START_FRAME

    start = AMQP::Frame.parse remote
    AMQP.parse_frame IO::Memory.new(start.to_slice)

    start_ok = AMQP::StartOk.new
    puts "start_ok #{start_ok.to_slice}"
    remote.write start_ok.to_slice

    tune = AMQP::Frame.parse remote
    puts "tune #{tune.to_slice}"

    tune_ok = AMQP::TuneOk.new
    puts "tune_ok #{tune_ok.to_slice}"
    remote.write tune_ok.to_slice

    open = AMQP::Open.new
    AMQP.parse_frame IO::Memory.new(open.to_slice)
    puts "open #{open.to_slice}"
    remote.write open.to_slice

    open_ok = AMQP::Frame.parse remote
    puts "open_ok #{open_ok.to_slice}"
  end

  def negotiate_client(socket)
    start = Bytes.new(8)
    bytes = socket.read_fully(start)

    if start != START_FRAME
      socket.write START_FRAME
      socket.close
      return
    end

    start = AMQP::Start.new
    AMQP.parse_frame IO::Memory.new(start.to_slice)
    socket.write start.to_slice

    start_ok = AMQP::Frame.parse socket
    AMQP.parse_frame IO::Memory.new start_ok.to_slice

    tune = AMQP::Tune.new
    socket.write tune.to_slice

    tune_ok = AMQP::Frame.parse socket
    AMQP.parse_frame IO::Memory.new tune_ok.to_slice

    open = AMQP::Frame.parse socket
    AMQP.parse_frame IO::Memory.new open.to_slice

    open_ok = AMQP::OpenOk.new
    socket.write open_ok.to_slice
  end

  def start
    server = TCPServer.new("localhost", 1234)
    loop do
      puts "Waiting for connections"
      if socket = server.accept?
        spawn handle_connection(socket)
      else
        break
      end
    end
  end
end

Proxy.start
