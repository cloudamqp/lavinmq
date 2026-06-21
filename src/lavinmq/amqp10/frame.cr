require "./codec"

module LavinMQ
  module AMQP10
    # 8-byte protocol headers used during version negotiation
    # (amqp-core-transport-v1.0 §2.2). "AMQP" + protocol-id + major.minor.rev.
    AMQP_PROTOCOL_HEADER = Bytes[0x41, 0x4d, 0x51, 0x50, 0x00, 0x01, 0x00, 0x00]
    SASL_PROTOCOL_HEADER = Bytes[0x41, 0x4d, 0x51, 0x50, 0x03, 0x01, 0x00, 0x00]

    # A single AMQP 1.0 frame (amqp-core-transport-v1.0 §2.3.1).
    #
    # Layout: 4-byte size, 1-byte data-offset (in 32-bit words), 1-byte type,
    # 2-byte type-specific channel, optional extended header, then the body.
    # The body is a described performative optionally followed by an opaque
    # payload (the message sections, for `transfer`).
    struct Frame
      TYPE_AMQP = 0x00_u8
      TYPE_SASL = 0x01_u8

      DOFF_MIN = 2_u8 # 2 words == 8 bytes, no extended header

      getter type : UInt8
      getter channel : UInt16
      getter performative : Described?
      getter payload : Bytes

      EMPTY_PAYLOAD = Bytes.empty

      def initialize(@performative : Described?, @channel : UInt16 = 0_u16,
                     @payload : Bytes = EMPTY_PAYLOAD, @type : UInt8 = TYPE_AMQP)
      end

      # An empty AMQP frame doubles as a heartbeat.
      def heartbeat? : Bool
        @performative.nil? && @payload.empty?
      end

      # Read a frame, allocating a fresh body buffer (cold paths / specs).
      def self.read(io : IO) : Frame
        read(io, IO::Memory.new)
      end

      # Read a frame reusing `buf` for the body — no per-frame allocation. The
      # returned `payload` and any decoded slices reference `buf`'s storage, so
      # they are only valid until `buf` is next reused (fine within the
      # single-fiber read loop that processes one frame before reading the next).
      def self.read(io : IO, buf : IO::Memory) : Frame
        size = UInt32.from_io(io, Codec::BE)
        raise Error::Decode.new("Frame size #{size} smaller than header") if size < 8
        doff = io.read_byte || raise Error::Decode.new("EOF reading data-offset")
        type = io.read_byte || raise Error::Decode.new("EOF reading frame type")
        channel = UInt16.from_io(io, Codec::BE)
        raise Error::Decode.new("Invalid data-offset #{doff}") if doff < DOFF_MIN
        # Skip any extended header.
        ext = (doff.to_i * 4) - 8
        io.skip(ext) if ext > 0
        body_size = size.to_i - (doff.to_i * 4)
        return Frame.new(nil, channel, EMPTY_PAYLOAD, type) if body_size <= 0
        buf.clear
        copied = IO.copy(io, buf, body_size)
        raise IO::EOFError.new if copied != body_size
        buf.rewind
        performative = Codec.read(buf).as(Described)
        payload = body_size - buf.pos > 0 ? buf.to_slice[buf.pos, body_size - buf.pos] : EMPTY_PAYLOAD
        Frame.new(performative, channel, payload, type)
      end
    end

    # Writes outbound frames. The body is produced by a block so performatives
    # can encode themselves with correct field widths (rather than going through
    # the generic `Codec` which can't distinguish uint from ulong).
    module FrameWriter
      extend self

      # Allocating overload for cold paths (handshake / specs).
      def write(io : IO, channel : UInt16 = 0_u16, type : UInt8 = Frame::TYPE_AMQP, & : IO::Memory ->) : Nil
        write(io, IO::Memory.new, channel, type) { |b| yield b }
      end

      # Hot-path overload: assemble the body into the caller-provided reusable
      # buffer `scratch` (cleared each call) instead of allocating one.
      def write(io : IO, scratch : IO::Memory, channel : UInt16 = 0_u16, type : UInt8 = Frame::TYPE_AMQP, & : IO::Memory ->) : Nil
        scratch.clear
        yield scratch
        size = 8_u32 + scratch.bytesize
        size.to_io(io, Codec::BE)
        io.write_byte Frame::DOFF_MIN
        io.write_byte type
        channel.to_io(io, Codec::BE)
        io.write(scratch.to_slice) if scratch.bytesize > 0
      end

      # Empty AMQP frame, used as a heartbeat.
      def heartbeat(io : IO) : Nil
        write(io) { }
      end
    end
  end
end
