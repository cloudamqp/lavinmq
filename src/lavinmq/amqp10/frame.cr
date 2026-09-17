require "./codec"
require "../config"

module LavinMQ::AMQP10
  struct Frame
    getter type, channel, body

    def initialize(@type : UInt8, @channel : UInt16, @body : Bytes, @reader : IO::Memory)
    end

    def body_reader : IO::Memory
      @reader.reset(@body)
    end
  end

  class FrameReader
    @header = Bytes.new(8)
    @buffer : Bytes
    @reader = IO::Memory.new(Bytes.empty)
    # Largest frame accepted, header included; never more than the buffer holds.
    getter max_frame_size : UInt32

    def initialize(@io : IO, max_frame_size : UInt32)
      size = max_frame_size.zero? ? Config.instance.frame_max : max_frame_size
      @max_frame_size = Math.max(size, MIN_MAX_FRAME_SIZE)
      @buffer = Bytes.new(@max_frame_size)
    end

    # Lowers the accepted frame size to the negotiated one without reallocating
    # the buffer, so the reader used for the Open handshake can carry on serving
    # the connection.
    def max_frame_size=(size : UInt32) : Nil
      size = Math.max(size, MIN_MAX_FRAME_SIZE)
      @max_frame_size = Math.min(size, @buffer.bytesize.to_u32)
    end

    def read : Frame
      @io.read_fully(@header)
      size = IO::ByteFormat::NetworkEndian.decode(UInt32, @header[0, 4])
      doff = @header[4]
      type = @header[5]
      channel = IO::ByteFormat::NetworkEndian.decode(UInt16, @header[6, 2])
      raise DecodeError.new("invalid AMQP 1.0 frame data offset #{doff}") if doff < 2
      raise DecodeError.new("invalid AMQP 1.0 frame size #{size}") if size < doff.to_u32 * 4
      raise DecodeError.new("AMQP 1.0 frame too large #{size}") if size > @max_frame_size
      remaining = size - 8
      slice = @buffer[0, remaining]
      @io.read_fully(slice)
      ext_size = doff.to_i * 4 - 8
      Frame.new(type, channel, slice[ext_size, remaining - ext_size], @reader)
    end
  end

  module FrameWriter
    extend self

    def write_frame_header(io : IO, size : UInt32, type : UInt8, channel : UInt16) : Nil
      Codec.write_u32(io, size)
      io.write_byte 2_u8
      io.write_byte type
      Codec.write_u16(io, channel)
    end

    def write_performative(io : IO, channel : UInt16, type : UInt8, code : UInt64, fields : Array(Value)) : Nil
      body_size = Codec.described_list_size(code, fields)
      write_frame_header(io, (8 + body_size).to_u32, type, channel)
      Codec.write_described_list(io, code, fields)
      io.flush
    end

    def write_empty_performative(io : IO, channel : UInt16, code : UInt64) : Nil
      write_performative(io, channel, AMQP_FRAME_TYPE, code, Array(Value).new)
    end
  end
end
