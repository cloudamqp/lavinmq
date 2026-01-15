require "io/buffered"

module LavinMQ
  class TLSSocketWrapper < IO
    Log = LavinMQ::Log.for "tls_socket_wrapper"
    include IO::Buffered

    @read_channel : Channel(Bytes) = Channel(Bytes).new(0)
    @write_channel : Channel(Bytes) = Channel(Bytes).new(0)
    @flush_channel : Channel(Nil) = Channel(Nil).new(0)
    @wait_channel : Channel(Int32 | Int64) = Channel(Int32 | Int64).new(0)

    def initialize(@socket : OpenSSL::SSL::Socket, @connection_info : ConnectionInfo)
      @socket.sync = true
      @socket.read_buffering = false
      spawn worker_loop, name: "TLS_socket_worker_loop for #{@socket.remote_address}"
      spawn reader_loop, name: "TLS_socket_reader_loop for #{@socket.remote_address}"
    end

    getter connection_info : ConnectionInfo

    private def reader_loop
      Log.context.set(fiber: Fiber.current.name)
      loop do
        slice = @read_channel.receive
        Log.info { "Reader loop read request: slice_size=#{slice.size}" }
        @wait_channel.send @socket.unbuffered_read(slice)
      end
    rescue ex : Channel::ClosedError
      @wait_channel.close rescue nil
    end

    private def worker_loop
      Log.context.set(fiber: Fiber.current.name)
      loop do
        select
        when slice = @write_channel.receive
          Log.info { "Worker loop write request: slice_size=#{slice.size}" }
          @socket.write(slice)
          Log.info { "Worker loop write completed slize size remaining=#{slice.size}" }
        when @flush_channel.receive
          Log.info { "Worker loop flush request" }
          @socket.flush
          Log.info { "Worker loop flush completed" }
        end
      end
    rescue ex : Channel::ClosedError
      # Nothing to do, exiting worker loop
    end

    private def unbuffered_read(slice : Bytes)
      Log.info { "Read: slice_size=#{slice.size} fiber: #{Fiber.current.name}" }
      @read_channel.send(slice)
      size = @wait_channel.receive
      # sleep(200.milliseconds) # Simulate some delay

      # size = @socket.read(slice)
      Log.info { "Read completed: size=#{size} fiber: #{Fiber.current.name}" }
      return size
    end

    private def unbuffered_write(slice : Bytes) : Nil
      Log.info { "Write: slice_size=#{slice.size}" }
      @write_channel.send(slice)
      # @socket.write(slice)
      return
    end

    private def unbuffered_rewind : Nil
      raise Socket::Error.new("Can't rewind")
    end

    private def unbuffered_close : Nil
      return if @closed

      @closed = true
      @read_channel.close
      @write_channel.close

      @wait_channel.receive? # Drain any pending wait
      @socket.close
    end

    private def unbuffered_flush : Nil
      @flush_channel.send(nil)
    end
  end
end
