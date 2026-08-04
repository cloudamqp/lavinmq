# Spec-only failure injection for MessageStore.
#
# Some error-recovery paths are only reachable when the message store itself
# fails (ENOSPC while appending to the acks file, a dropped segment, etc.),
# which can't be provoked through the public API. Set `raise_on_delete_after`
# to let N deletes succeed and have every later delete raise, so specs can
# exercise a failure that happens *partway* through a batch of acks.
class LavinMQ::MessageStore
  property raise_on_delete_after : Int32? = nil
  @spec_delete_count = 0

  def delete(sp) : Nil
    if after = @raise_on_delete_after
      raise IO::Error.new("spec: injected message store delete failure") if @spec_delete_count >= after
      @spec_delete_count += 1
    end
    previous_def
  end
end
