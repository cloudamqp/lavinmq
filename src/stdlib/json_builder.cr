class JSON::Builder
  # Writes a raw scalar from the block, like `raw(String)` without building a String.
  # The block must write a valid JSON value.
  def raw(& : IO ->) : Nil
    scalar do
      yield @io
    end
  end
end
