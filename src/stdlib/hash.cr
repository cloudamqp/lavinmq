class Hash(K, V)
  def capacity
    entries_capacity + indices_size * @indices_bytesize
  end

  # PR: https://github.com/crystal-lang/crystal/pull/14862
  private def delete_entry(index) : Nil
    # sets `Entry#@hash` to 0 and removes stale references to key and value
    (@entries + index).clear
  end
end
