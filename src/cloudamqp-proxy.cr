require "socket"
require "./cloudamqp-proxy/amqp"
require "./cloudamqp-proxy/version"

module Proxy
  extend self

  def create_token_bucket(length, interval)
    bucket = Channel::Buffered(nil).new(length)
    spawn do
      loop do
        length.times do
          break if bucket.full?
          bucket.send nil
        end
        sleep interval
      end
    end
    bucket
  end

  def copy(i, o)
    bucket = create_token_bucket(10, 1.seconds)
    loop do
      bucket.receive # block waiting for tokens
      frame = AMQP::Frame.decode i
      if frame.type == AMQP::Type::Heartbeat
        i.write frame.to_slice
      else
        o.write frame.to_slice
      end
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
    puts "socket#sync=#{socket.sync?} socket#send_buffer_size=#{socket.send_buffer_size} socket#recv_buffer_size=#{socket.recv_buffer_size}"
    negotiate_client(socket)

    remote = TCPSocket.new("localhost", 5672)
    puts "remote#sync=#{remote.sync?} remote#send_buffer_size=#{remote.send_buffer_size} socket#recv_buffer_size=#{remote.recv_buffer_size}"
    negotiate_server(remote)

    spawn copy(remote, socket)
    spawn copy(socket, remote)
  rescue ex : IO::EOFError | Errno
    puts "handle_connection #{ex}"
    remote.close if remote
    socket.close
  end

  def negotiate_server(remote)
    remote.write AMQP::PROTOCOL_START

    start = AMQP::Frame.decode remote

    start_ok = AMQP::Connection::StartOk.new
    puts "start_ok #{start_ok.to_slice}"
    remote.write start_ok.to_slice

    tune = AMQP::Frame.decode remote
    puts "tune #{tune.to_slice}"

    tune_ok = AMQP::Connection::TuneOk.new(heartbeat: 0_u16)
    puts "tune_ok #{tune_ok.to_slice}"
    remote.write tune_ok.to_slice

    open = AMQP::Connection::Open.new
    puts "open #{open.to_slice}"
    remote.write open.to_slice

    open_ok = AMQP::Frame.decode remote
    puts "open_ok #{open_ok.to_slice}"
  end

  def negotiate_client(socket)
    start = Bytes.new(8)
    bytes = socket.read_fully(start)

    if start != AMQP::PROTOCOL_START
      socket.write AMQP::PROTOCOL_START
      socket.close
      return
    end

    puts "sending start"
    start = AMQP::Connection::Start.new
    socket.write start.to_slice
    puts "sent start"

    puts "reading start_ok"
    start_ok = AMQP::Frame.decode socket
    puts "read start_ok"

    puts "sending tune"
    tune = AMQP::Connection::Tune.new(heartbeat: 0_u16)
    socket.write tune.to_slice
    puts "sent tune"

    tune_ok = AMQP::Frame.decode socket

    open = AMQP::Frame.decode socket

    open_ok = AMQP::Connection::OpenOk.new
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
