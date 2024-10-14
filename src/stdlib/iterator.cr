module Iterator(T)
  def self.empty
    EmptyIterator(T).new
  end

  private struct EmptyIterator(T)
    include Iterator(T)

    def next
      stop
    end
  end
end
