module LavinMQ
  class JournalErrorWriter < IO::FileDescriptor
    def initialize
      super(STDERR.fd, blocking: STDERR.blocking)
    end

    def write(slice : Bytes) : Nil
      self.write_string "<3> ".to_slice
      super slice
    end
  end
end
