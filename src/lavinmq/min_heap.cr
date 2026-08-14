module LavinMQ
  class MinHeap(T)
    def initialize
      @heap = Array(T).new
    end

    def size
      @heap.size
    end

    def empty?
      @heap.empty?
    end

    def first? : T?
      @heap.first?
    end

    def push(value : T) : Nil
      @heap << value
      sift_up(@heap.size - 1)
    end

    def shift? : T?
      return if @heap.empty?
      min = @heap.first
      last = @heap.pop
      unless @heap.empty?
        @heap[0] = last
        sift_down(0)
      end
      min
    end

    def clear : Nil
      @heap = Array(T).new
    end

    private def sift_up(idx : Int32) : Nil
      value = @heap[idx]
      while idx > 0
        parent = (idx - 1) // 2
        break if @heap[parent] <= value
        @heap[idx] = @heap[parent]
        idx = parent
      end
      @heap[idx] = value
    end

    private def sift_down(idx : Int32) : Nil
      value = @heap[idx]
      size = @heap.size
      while (child = 2 * idx + 1) < size
        right = child + 1
        child = right if right < size && @heap[right] < @heap[child]
        break if value <= @heap[child]
        @heap[idx] = @heap[child]
        idx = child
      end
      @heap[idx] = value
    end
  end
end
