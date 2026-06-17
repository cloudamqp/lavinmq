module LavinMQ
  module MQTT
    # Splits a buffer on a delimiter byte, yielding each part as a `Bytes` view
    # into the buffer (no allocation per token). Empty parts are returned, so
    # "/" yields two empty tokens and "a//b" yields "a", "" and "b".
    #
    # The delimiter must be a single ASCII byte; splitting on ASCII over a UTF-8
    # buffer is safe because UTF-8 multi-byte sequences only use bytes >= 0x80.
    struct BytesTokenIterator
      def initialize(@bytes : Bytes, delimiter : Char = '/')
        @delimiter = delimiter.ord.to_u8
        @pos = 0
        @iteration = 0
      end

      def next : Bytes?
        return if @pos >= @bytes.size
        # Skip the previous token's delimiter, except on the first iteration so
        # a leading delimiter yields an empty token.
        @pos += 1 unless @iteration.zero?
        @iteration += 1
        head = @pos
        while @pos < @bytes.size && @bytes.unsafe_fetch(@pos) != @delimiter
          @pos += 1
        end
        @bytes[head, @pos - head]
      end

      def next?
        @pos < @bytes.size
      end
    end
  end
end
