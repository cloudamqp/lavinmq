module Iterator(T)
  # Bugfix for crystal-lang/crystal/issues/16922
  def compact_map(&func : T -> U?) forall U
    x = uninitialized T
    CompactMapIterator(typeof(self), T, typeof(func.call(x).not_nil!)).new(self, func)
  end
end
