#
# str = "my/example/string"
# it = StringTokenIterator.new(str, '/')
# while substr = it.next
#   puts substr
# end
# outputs:
#  my
#  example
#  string
#
# Note that "empty" parts will also be returned
# str = "/" will result in two ""
# str "a//b" will result in "a", "" and "b"
#
module LavinMQ
  module MQTT
    struct StringTokenIterator
      def initialize(@str : String, @delimiter : Char = '/')
        @reader = Char::Reader.new(@str)
        @iteration = 0
      end

      def next : String?
        return if @reader.pos >= @str.size
        # This is to make sure we return an empty string first iteration if @str starts with @delimiter
        @reader.next_char unless @iteration.zero?
        @iteration += 1
        head = @reader.pos
        while @reader.has_next? && @reader.current_char != @delimiter
          @reader.next_char
        end
        tail = @reader.pos
        @str[head, tail - head]
      end

      def next?
        @reader.pos < @str.size
      end

      def to_s
        @str
      end

      def inspect
        "#{self.class.name}(@str=#{@str} @reader.pos=#{@reader.pos} @reader.current_char=#{@reader.current_char} @iteration=#{@iteration})"
      end
    end
  end
end
