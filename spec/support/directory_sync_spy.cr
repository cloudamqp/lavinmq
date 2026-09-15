class LavinMQ::FileSystem::Directory
  getter sync_count = 0
  property before_sync : Proc(Nil)?

  def fsync : Nil
    @before_sync.try &.call
    previous_def
    @sync_count += 1
  end
end
