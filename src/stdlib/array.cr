class Array(T)
  def capacity
    @capacity
  end

  def insert_sorted(v : T)
    i = bsearch_index { |x| x >= v }
    if i
      insert(i, v) unless self.[i] == v
    else
      push(v)
    end
  end
end

class SortedSet(T)
  def initialize
    @array = Array(T).new
  end

  def push(v : T)
    i = @array.bsearch_index { |x| x >= v }
    if i
      if @array[i] != v
        @array.insert(i, v)
      end
    else
      @array.push(v)
    end
  end

  def <<(v : T)
    push(v)
  end


  def each(&blk : T -> Nil)
    @array.each(&blk)
  end

  def each
    @array.each
  end

  def clear
    @array.clear
  end

  def size
    @array.size
  end

  def capacity
    @array.capacity
  end

  def empty?
    @array.empty?
  end

  def first
    @array.first
  end
end
