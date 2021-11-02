class Hash(K, V)
  def capacity
    entries_capacity + indices_size * @indices_bytesize
  end
end
