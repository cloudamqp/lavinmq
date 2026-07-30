require "./bytes_token_iterator"
require "./topic_filter"

module LavinMQ
  module MQTT
    # BytesTokenIterator is deliberately a struct: passing it to the next
    # segment copies the cursor, so a segment never disturbs its caller's
    # position.
    abstract class TopicRuleSegment
      abstract def match?(it : BytesTokenIterator, client_id : String) : Bool

      def self.compile(pattern : String) : TopicRuleSegment?
        return nil unless TopicFilter.valid_filter?(pattern)
        chain = nil.as(TopicRuleSegment?)
        pattern.split('/').reverse_each do |level|
          chain = case level
                  when "#"           then HashRuleSegment.new
                  when "+"           then PlusRuleSegment.new(chain)
                  when "{client_id}" then ClientIdRuleSegment.new(chain)
                  else                    StringRuleSegment.new(level.to_slice, chain)
                  end
        end
        chain
      end

      def self.matches?(chain : TopicRuleSegment, topic : String, client_id : String) : Bool
        chain.match?(BytesTokenIterator.new(topic.to_slice), client_id)
      end

      # Shared tail handling: either hand the advanced cursor to the next segment,
      # or require that the topic ended here.
      protected def match_rest?(nxt : TopicRuleSegment?, it : BytesTokenIterator, client_id : String) : Bool
        if nxt
          nxt.match?(it, client_id)
        else
          !it.next?
        end
      end
    end

    # One level equal to a fixed byte string.
    class StringRuleSegment < TopicRuleSegment
      def initialize(@s : Bytes, @next : TopicRuleSegment?)
      end

      def match?(it : BytesTokenIterator, client_id : String) : Bool
        token = it.next
        return false unless token
        return false unless token == @s
        match_rest?(@next, it, client_id)
      end
    end

    # Any single level.
    class PlusRuleSegment < TopicRuleSegment
      def initialize(@next : TopicRuleSegment?)
      end

      def match?(it : BytesTokenIterator, client_id : String) : Bool
        return false unless it.next
        match_rest?(@next, it, client_id)
      end
    end

    # All remaining levels, including none. `compile` rejects a filter with '#'
    # anywhere but the last level, so this segment never has a successor.
    class HashRuleSegment < TopicRuleSegment
      def match?(it : BytesTokenIterator, client_id : String) : Bool
        true
      end
    end

    # One level equal to the requesting client id. A client id containing
    # '/', '+' or '#' cannot widen the filter, it simply never matches.
    class ClientIdRuleSegment < TopicRuleSegment
      def initialize(@next : TopicRuleSegment?)
      end

      def match?(it : BytesTokenIterator, client_id : String) : Bool
        token = it.next
        return false unless token
        return false unless token == client_id.to_slice
        match_rest?(@next, it, client_id)
      end
    end
  end
end
