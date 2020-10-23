class Array(T)
  def capacity
    @capacity
  end

  def delete_if
    reject! { |e| yield e }
  end
end
