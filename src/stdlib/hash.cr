class Hash(K, V)
  def capacity
    indices_size + entries_size
  end
end
