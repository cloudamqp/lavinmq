require "socket"

class Socket
  # Zero-copy implementation of sending *limit* bytes of *file*
  # to the socket
  # Returns the number of bytes sent
  def sendfile(file : IO::FileDescriptor, limit : Int) : Int64
    flush
    file.seek(0, IO::Seek::Current) unless file.@in_buffer_rem.empty?
    evented_sendfile(limit, "sendfile") do |remaining|
      {% if flag?(:linux) %}
        LibC.sendfile(fd, file.fd, nil, remaining)
      {% else %}
        len = pointerof(remaining)
        LibC.sendfile(file.fd, fd, nil, len, nil, 0)
        len.value
      {% end %}
    end
  end

  struct IPAddress
    def loopback? : Bool
      case addr = @addr
      in LibC::InAddr
        addr.s_addr & 0x000000ff_u32 == 0x0000007f_u32
      in LibC::In6Addr
        bytes = ipv6_addr8(addr)
        bytes == StaticArray[0_u8, 0_u8, 0_u8, 0_u8, 0_u8, 0_u8, 0_u8, 0_u8, 0_u8, 0_u8, 0_u8, 0_u8, 0_u8, 0_u8, 0_u8, 1_u8] ||
          bytes.to_slice[0, 13] == Bytes[0_u8, 0_u8, 0_u8, 0_u8, 0_u8, 0_u8, 0_u8, 0_u8, 0_u8, 0_u8, 255_u8, 255_u8, 127_u8]
      end
    end
  end
end

module IO::Evented
  def evented_sendfile(limit : Int, errno_msg : String, &) : Int64
    limit = limit.to_i64
    remaining = limit
    loop do
      bytes_written = (yield remaining).to_i32
      case bytes_written
      when -1
        if Errno.value == Errno::EAGAIN
          wait_writable
        else
          raise Socket::Error.from_errno(errno_msg)
        end
      when 0
        break
      else
        remaining -= bytes_written
        break if remaining.zero?
      end
    end
    limit - remaining
  ensure
    resume_pending_writers
  end
end
