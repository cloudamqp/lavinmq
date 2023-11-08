struct Crystal::FiberChannel
  def close
    # Send zero pointer to signal stop
    @worker_in.write_bytes 0u64
  end

  def receive : Fiber?
    oid = @worker_out.read_bytes(UInt64)
    if oid.zero?
      @worker_in.close
      @worker_out.close
      nil
    else
      Pointer(Fiber).new(oid).as(Fiber)
    end
  end
end
