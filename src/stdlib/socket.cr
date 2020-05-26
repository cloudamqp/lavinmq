require "socket"

class Socket
  # Zero-copy implementation of sending *limit* bytes of *file*
  # to the socket
  # Returns the number of bytes sent
  def sendfile(file : IO::FileDescriptor, limit : Int) : UInt64
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
end

module IO::Evented
  def evented_sendfile(limit : Int, errno_msg : String) : UInt64
    limit = limit.to_u64
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
