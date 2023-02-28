require "./libc"

class IO::FileDescriptor
  def write_at(buffer, offset) : Int64
    bytes_written = LibC.pwrite(fd, buffer, buffer.size, offset)

    if bytes_written == -1
      raise IO::Error.from_errno "Error writing file"
    end

    bytes_written.to_i64
  end
end
