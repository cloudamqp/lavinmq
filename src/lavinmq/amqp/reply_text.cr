module LavinMQ
  module AMQP
    # A reply_text is sent as an AMQP short string, so it can never exceed 255
    # bytes. It embeds client-supplied names and exception messages, so it is
    # capped here rather than trusting every call site to be short. Encoding an
    # over-long short string raises part-way through the frame, leaving a
    # truncated frame on the socket and breaking the connection.
    module ReplyText
      MAX_BYTESIZE = 255

      def self.build(code, message) : String
        truncate("#{code} - #{message}")
      end

      # `String#byte_slice` would split a multi-byte character at the limit and
      # `String#scrub` would then expand it past the limit again, so step back
      # over any continuation bytes to land on a codepoint boundary.
      private def self.truncate(text : String) : String
        return text if text.bytesize <= MAX_BYTESIZE
        bytes = text.to_slice
        size = MAX_BYTESIZE
        while size > 0 && (bytes[size] & 0xC0) == 0x80
          size -= 1
        end
        String.new(bytes[0, size])
      end
    end
  end
end
