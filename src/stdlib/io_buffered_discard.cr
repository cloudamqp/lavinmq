module IO::Buffered
  # Drops data buffered for writing, so that a following `close` doesn't
  # flush it. Used when abandoning a connection: after a failed write the
  # buffer still holds data that may already have been partly sent.
  def discard_write_buffer : Nil
    @out_count = 0
  end
end
