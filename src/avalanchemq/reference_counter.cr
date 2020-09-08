module AvalancheMQ
  # A reference counter which performs an action
  # when the counter goes down to zero again
  class ReferenceCounter(T)
    @lock = Mutex.new(:unchecked)
    @counter = Deque(T).new(131_072)
    @zero_refs = Array(T).new(131_072)

    def []=(k : T, v : Int) : Nil
      @lock.synchronize do
        if idx = @counter.bsearch_index { |x| x > k }
          v.times { |i| @counter.insert(idx + i, k) }
        else
          v.times { @counter.push(k) }
        end
      end
    end

    def inc(k : T) : Nil
      @lock.synchronize do
        if idx = @counter.bsearch_index { |x| x > k }
          @counter.insert(idx, k)
        else
          @counter.push(k)
        end
      end
    end

    def dec(k : T) : Nil
      @lock.synchronize do
        if idx = @counter.bsearch_index { |x| x >= k }
          @counter.delete_at idx
          # if no more occurences
          unless @counter[idx]? == k
            @zero_refs.push k
          end
        else
          raise KeyError.new("Missing key #{k}")
        end
      end
    end

    # Yield and delete all zero referenced keys
    def empty_zero_referenced! : Nil
      @lock.synchronize do
        @zero_refs.each do |sp|
          yield sp
        end
        @zero_refs = Array(T).new(131_072)
        @counter = Deque(T).new(@counter.size) { |i| @counter[i] }
      end
    end

    def referenced_segments(set) : Nil
      @lock.synchronize do
        prev = nil
        @counter.each do |sp|
          if sp.segment != prev
            set << sp.segment
            prev = sp.segment
          end
        end
      end
    end

    def size
      @counter.size + @zero_refs.size
    end

    def capacity
      @counter.capacity + @zero_refs.capacity
    end
  end
end
