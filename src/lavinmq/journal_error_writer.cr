module LavinMQ
  class JournalErrorWriter < IO::FileDescriptor
    def initialize
      super(STDERR.fd, blocking: STDERR.blocking)
    end

    def write(slice : Bytes) : Nil
      self.write_byte(60_u8) # '<'
      self.write_byte(51_u8) # '3'
      self.write_byte(62_u8) # '>'
      self.write_byte(32_u8) # ' ' (space)
      super slice
    end
  end
end
