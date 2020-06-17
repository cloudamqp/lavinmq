require "./durable_queue"

module AvalancheMQ
  class PersistentExchangeQueue < DurableQueue
    @internal = true
    def initialize(vhost : VHost, name : String)
      super(vhost, name, false, false, Hash(String, AMQP::Field).new)

      # TODO Apply policies for max-length and ttl
    end

    def peek(c : Int, &blk : SegmentPosition -> Nil)
      return if @ready.empty?
      return if c <= 0
      size = @ready.size
      if c >= size
        @ready.each { |sp| yield sp }
      else
        i = -1
        skip = size - c
        @ready.each do |sp|
          i += 1
          next if i < skip
          yield sp
        end
      end
    end
  end
end
