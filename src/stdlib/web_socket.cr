require "http/web_socket"

class HTTP::WebSocket
  def send(message : Bytes) : Nil
    check_open
    @ws.send(message)
  end
end

class HTTP::WebSocket::Protocol
  # See https://github.com/crystal-lang/crystal/issues/16532
  class StreamIO < IO
    def flush : Nil
    end

    def flush(final : Bool)
      Log.debug { "flush final=#{final}" }
      @websocket.send(
        @buffer[0...@pos],
        @opcode,
        flags: final ? Flags::FINAL : Flags::None,
        flush: final
      )
      @opcode = Opcode::CONTINUATION
      @pos = 0
    end
  end

  def stream(binary = true, frame_size = 1024, &)
    stream_io = StreamIO.new(self, binary, frame_size)
    yield(stream_io)
    stream_io.flush(final: true)
  end
end
