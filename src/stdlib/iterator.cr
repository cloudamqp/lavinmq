module Iterator(T)
  # Bugfix for crystal-lang/crystal/issues/16922
  # Probably not 100%, but as long as it compiles...
  def compact_map(&func : T -> U) forall U
    x = uninitialized T
    # ameba:disable Lint/NotNil
    CompactMapIterator(typeof(self), T, typeof(func.call(x).not_nil!)).new(self, func)
  end

  private struct CompactMapIterator(I, T, U)
    def self.new(iterator, func : T -> U)
      MapIterator(I, T, U).new(iterator, func)
    end

    # We must redeclare constructor to make it come AFTER self.new
    def initialize(@iterator : I, @func : T -> U?)
    end
  end
end
