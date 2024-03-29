module LavinMQ
  struct SegmentPosition
    include Comparable(self)

    getter segment : UInt32
    getter position : UInt32
    getter bytesize : UInt32
    getter delay : UInt32   # used by delayed exchange queue
    getter priority : UInt8 # required for ordering in priority queues
    getter? has_dlx : Bool

    def_equals_and_hash @segment, @position

    def initialize(@segment : UInt32, @position : UInt32, @bytesize : UInt32, @has_dlx = false, @priority = 0u8, @delay = 0u32)
    end

    def self.make(segment : UInt32, position : UInt32, msg)
      prio = msg.properties.priority || 0u8
      has_dlx = false
      delay = 0u32
      msg.properties.headers.try &.each do |key, value|
        case key
        when "x-dead-letter-exchange" then has_dlx = true
        when "x-delay"                then delay = value.as?(Int).try(&.to_u32) || 0u32 rescue 0u32
        end
      end
      self.new(segment, position, msg.bytesize.to_u32, has_dlx, prio, delay)
    end

    def <=>(other : self)
      r = segment <=> other.segment
      return r unless r.zero?
      position <=> other.position
    end

    def to_s(io : IO)
      io << @segment.to_s.rjust(10, '0')
      io << @position.to_s.rjust(10, '0')
    end
  end
end
