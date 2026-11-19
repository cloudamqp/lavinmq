require "socket"
require "./gc"

# `finalize` is a no-op once closed, so drop it on close
class Socket
  private def unbuffered_close : Nil
    return if @closed

    begin
      previous_def
    ensure
      GC.remove_finalizer(self)
    end
  end
end

class IO::FileDescriptor
  private def unbuffered_close : Nil
    return if @closed

    begin
      previous_def
    ensure
      GC.remove_finalizer(self)
    end
  end
end
