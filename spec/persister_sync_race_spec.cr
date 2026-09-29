require "./spec_helper"

private class BlockingSyncPersister < LavinMQ::Persister
  getter syncing = Channel(Nil).new(1)
  getter resume = Channel(Nil).new(1)

  protected def sync_file(file : MFile) : Nil
    @syncing.send nil
    @resume.receive
    super
  end
end

describe LavinMQ::Persister do
  it "waits for an in-flight sync even after it has drained the dirty set" do
    with_datadir do |dir|
      persister = BlockingSyncPersister.new(data_dir: dir)
      file = MFile.new(File.join(dir, "msgs"), 4096)
      file.write "body".to_slice
      persister.mark_dirty(file)
      done = Channel(Nil).new(2)
      spawn { persister.sync; done.send nil }
      persister.syncing.receive
      spawn { persister.sync; done.send nil }
      select
      when done.receive
        fail "sync returned before the dirty file was persisted"
      when timeout(20.milliseconds)
      end
      persister.resume.send nil
      2.times { done.receive }
    ensure
      persister.try &.resume.try_send(nil)
      persister.try &.close
      file.try &.close
    end
  end
end
