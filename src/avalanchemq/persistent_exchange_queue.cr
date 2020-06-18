require "./durable_queue"

module AvalancheMQ
  class PersistentExchangeQueue < DurableQueue
    @internal = true
    def initialize(vhost : VHost, name : String, limit : ArgumentNumber)
      super(vhost, name, false, false, Hash(String, AMQP::Field).new)

      pd = Hash(String, JSON::Any).new
      pd["max-length"] = JSON::Any.new(Int64.new(limit))

      # pd["message-ttl"] = 2

      persistent_policy = Policy.new(
        "persistent_queue",
        vhost.name,
        Regex.new(name),
        Policy::Target::Queues,
        pd,
        0)
      apply_policy(persistent_policy)
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
