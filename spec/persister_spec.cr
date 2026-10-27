require "./spec_helper"

private def queue_dir(s : LavinMQ::Server, queue_name : String) : String
  File.join(s.vhosts["/"].data_dir, Digest::SHA1.hexdigest(queue_name))
end

private def last_sync(s : LavinMQ::Server) : LavinMQ::Persister::SyncRecord
  s.persister.last_sync.should_not be_nil
end

private class ConfirmReceiver
  include LavinMQ::Persister::ConfirmTarget
  getter confirms = Channel(UInt64).new(1)

  def enqueue_confirm_ack(msgid : UInt64) : Nil
    @confirms.send msgid
  end
end

describe LavinMQ::Persister do
  it "syncs the segments of confirmed publishes, not of other publishes" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        plain = ch.queue("plain")
        confirmed = ch.queue("confirmed")
        plain.publish "not confirmed"
        confirmed.publish_confirm "confirmed"
        sync = last_sync(s)
        sync.syncfs.should be_false
        sync.paths.should contain File.join(queue_dir(s, "confirmed"), "msgs.0000000001")
        sync.paths.none?(&.starts_with?(queue_dir(s, "plain"))).should be_true
      end
    end
  end

  it "fsyncs the directory of a newly created segment once" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("new_segment")
        q.publish_confirm "first"
        last_sync(s).paths.should contain queue_dir(s, "new_segment")
        q.publish_confirm "second"
        last_sync(s).paths.should_not contain queue_dir(s, "new_segment")
        last_sync(s).paths.should contain File.join(queue_dir(s, "new_segment"), "msgs.0000000001")
      end
    end
  end

  it "doesn't sync the ack files consumers append to" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("acked")
        10.times { |i| q.publish "m#{i}" }
        10.times do
          msg = q.get(no_ack: false).should_not be_nil
          msg.ack
        end
        should_eventually(be_true) { s.vhosts["/"].queue("acked").message_count.zero? }
        q.publish_confirm "confirmed"
        last_sync(s).paths.none?(&.includes?("acks.")).should be_true
      end
    end
  end

  it "syncs the whole filesystem on tx commit, so the tx acks are durable too" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("tx")
        q.publish "m"
        ch.tx_select
        msg = q.get(no_ack: false).should_not be_nil
        msg.ack
        q.publish "tx publish"
        ch.tx_commit
        last_sync(s).syncfs.should be_true
      end
    end
  end

  it "syncs the whole filesystem when a confirm depends on more files than the threshold" do
    with_amqp_server do |s|
      LavinMQ::Config.instance.syncfs_threshold = 4
      with_channel(s) do |ch|
        x = ch.exchange("many", "fanout")
        (LavinMQ::Config.instance.syncfs_threshold + 1).times do |i|
          ch.queue("many#{i}").bind(x.name, "")
        end
        ch.confirm_select
        x.publish_confirm "to all", ""
        last_sync(s).syncfs.should be_true
      end
    end
  end

  it "syncs the whole data dir instead of exiting when it's out of file descriptors" do
    with_datadir do |data_dir|
      persister = LavinMQ::Persister.new(data_dir)
      target = ConfirmReceiver.new
      # A new segment, whose directory the drain fsyncs by opening it
      segment = MFile.new(File.join(data_dir, "msgs.0000000001"), 1024)
      begin
        segment.write "m".to_slice
        persister.mark_dirty(segment)
        without_free_file_descriptors do
          persister.enqueue_ack(target, 1u64)
          select
          when id = target.confirms.receive
            id.should eq 1u64
          when timeout(2.seconds)
            fail "the publish was never confirmed"
          end
        end
        persister.last_sync.should_not(be_nil).syncfs.should be_true
      ensure
        segment.close
        persister.close
      end
    end
  end
end
