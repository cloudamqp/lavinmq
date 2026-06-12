module LavinMQ::AMQP
  # Stateless helpers for the consumer-offset entry encoding and size-capping
  module ConsumerOffsets
    ENTRY_OVERHEAD = 1 + 8                # ShortString length byte + Int64 offset
    MAX_ENTRY_SIZE = 255 + ENTRY_OVERHEAD # Largest possible entry: 255-byte tag + Int64

    # Encoded size of one {consumer_tag, offset} entry in the offsets file.
    def self.entry_size(consumer_tag : String) : Int64
      (consumer_tag.bytesize + ENTRY_OVERHEAD).to_i64
    end

    # Drops the oldest (lowest position) offsets until the set fits in
    # `max_size`. Survivors are returned oldest first so rewriting them keeps
    # file position reflecting commit recency. Usually drops nothing.
    def self.trim_to_size(offsets : Array(Tuple(String, Int64, Int64)),
                          max_size : Int64) : Array(Tuple(String, Int64, Int64))
      offsets.sort_by! { |_ctag, _offset, pos| pos } # oldest (lowest position) first

      total_size = offsets.sum { |ctag, _offset, _pos| entry_size(ctag) }
      drop_count = 0
      # `>=` so the kept set stays strictly below the cap, leaving headroom.
      while total_size >= max_size
        total_size -= entry_size(offsets[drop_count][0])
        drop_count += 1
      end
      offsets[drop_count..]
    end
  end
end
