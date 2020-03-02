class StringPool
  def clear
    @capacity = 256
    @hashes = Pointer(UInt64).malloc(@capacity, 0_u64)
    @values = Pointer(String).malloc(@capacity, "")
    @size = 0
  end
end
