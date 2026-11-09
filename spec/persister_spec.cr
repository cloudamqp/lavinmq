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

  it "fsyncs the vhost directory once with the first confirm after queues are declared" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        3.times { |i| ch.queue("declared#{i}") }
        ch.queue("declared0").publish_confirm "first"
        last_sync(s).paths.count(s.vhosts["/"].data_dir).should eq 1
        ch.queue("declared0").publish_confirm "second"
        last_sync(s).paths.should_not contain s.vhosts["/"].data_dir
      end
    end
  end

  it "fsyncs the directory entries and .metadata of a new durable MQTT session with the next confirm" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("confirmed")
        q.publish_confirm "first" # syncs what earlier declares marked
        session_name = "#{LavinMQ::MQTT::SESSION_PREFIX}client1"
        s.vhosts["/"].declare_queue(session_name, true, false,
          LavinMQ::AMQP::Table.new({"x-queue-type": "mqtt"}))
        q.publish_confirm "second"
        paths = last_sync(s).paths
        paths.should contain s.vhosts["/"].data_dir
        paths.should contain queue_dir(s, session_name)
        paths.should contain File.join(queue_dir(s, session_name), ".metadata")
      end
    end
  end

  it "doesn't fsync the vhost directory for queues loaded on start" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        ch.queue("loaded").publish_confirm "before restart"
      end
      restart_server(s)
      with_channel(s) do |ch|
        ch.queue("loaded").publish_confirm "after restart"
        last_sync(s).paths.should_not contain s.vhosts["/"].data_dir
      end
    end
  end

  it "fsyncs the directory of a new priority queue's sub stores with the first confirm, not after a restart" do
    with_amqp_server do |s|
      args = AMQP::Client::Arguments.new({"x-max-priority" => 2})
      with_channel(s) do |ch|
        ch.queue("prio", args: args).publish_confirm "before restart"
        last_sync(s).paths.should contain queue_dir(s, "prio")
      end
      restart_server(s)
      with_channel(s) do |ch|
        ch.queue("prio", args: args).publish_confirm "after restart"
        last_sync(s).paths.should_not contain queue_dir(s, "prio")
      end
    end
  end

  it "doesn't fsync the directory of a transient priority queue" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        ch.queue("transient_prio", durable: false, args: AMQP::Client::Arguments.new({"x-max-priority" => 2}))
        ch.queue("confirmed").publish_confirm "first"
        transient_dir = File.join(s.vhosts["/"].data_dir, "transient", Digest::SHA1.hexdigest("transient_prio"))
        last_sync(s).paths.should_not contain transient_dir
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

  it "syncs a finished stream segment and its meta file without waiting for a confirm" do
    with_datadir do |data_dir|
      persister = LavinMQ::Persister.new(data_dir)
      store = LavinMQ::AMQP::StreamMessageStore.new(data_dir, nil, persister: persister)
      begin
        store.@segments[1u32].take_created! # as if an earlier confirm already synced its directory
        body = "x" * (LavinMQ::Config.instance.segment_size // 4)
        4.times { store.push(LavinMQ::Message.new("", "rk", body)) } # not confirmed, rolls over once
        store.@segments.size.should eq 2
        sync = wait_for { persister.last_sync }
        sync.paths.should contain File.join(data_dir, "msgs.0000000001")
        sync.paths.should contain File.join(data_dir, "meta.0000000001")
        sync.paths.should contain data_dir # holds the entry of msgs.0000000002
      ensure
        store.close
        persister.close
      end
    end
  end

  it "syncs the filesystem on the first drain after sync is enabled again" do
    with_datadir do |data_dir|
      persister = LavinMQ::Persister.new(data_dir)
      begin
        file = File.join(data_dir, "f")
        File.write(file, "x")
        LavinMQ::Config.instance.sync = false
        persister.mark_dirty(file)
        persister.flush # clears the marks without syncing
        LavinMQ::Config.instance.sync = true
        persister.flush
        persister.last_sync.should_not(be_nil).syncfs.should be_true
        persister.flush
        persister.last_sync.should_not(be_nil).syncfs.should be_false
      ensure
        persister.close
      end
    end
  end

  it "leaves finished stream segments to the kernel when sync is disabled" do
    with_datadir do |data_dir|
      LavinMQ::Config.instance.sync = false
      persister = LavinMQ::Persister.new(data_dir)
      store = LavinMQ::AMQP::StreamMessageStore.new(data_dir, nil, persister: persister)
      begin
        body = "x" * (LavinMQ::Config.instance.segment_size // 4)
        4.times { store.push(LavinMQ::Message.new("", "rk", body)) }
        store.@segments.size.should eq 2
        persister.@pending.lock { |batch| batch.files.empty? && batch.paths.empty? }.should be_true
        persister.last_sync.should be_nil
      ensure
        store.close
        persister.close
      end
    end
  end

  it "stops collecting paths at the threshold and syncs the whole filesystem instead" do
    with_amqp_server do |s|
      LavinMQ::Config.instance.syncfs_threshold = 4
      with_channel(s) do |ch|
        # Transient, so its confirms mark no segment and only the flag can
        # make the drain sync the whole filesystem
        q = ch.queue("confirmed", durable: false)
        q.publish_confirm "first" # syncs what earlier declares marked
        # Never confirmed, like on a server with QoS 0 only
        10.times do |i|
          s.vhosts["/"].declare_queue("prio#{i}", true, false, LavinMQ::AMQP::Table.new({"x-max-priority": 2}))
        end
        s.persister.@pending.lock(&.paths.size).should eq 4
        q.publish_confirm "second"
        last_sync(s).syncfs.should be_true
      end
    end
  end
end

describe LavinMQ::Persister, "messages the broker moves between queues" do
  it "syncs the segments of the moved copies, not the filesystem, before an emptied segment is unlinked" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        ch.queue("dst")
        src = ch.queue("src", args: AMQP::Client::Arguments.new({
          "x-message-ttl"             => 1,
          "x-dead-letter-exchange"    => "",
          "x-dead-letter-routing-key" => "dst",
        }))
        # Plain publishes: no confirm requests a sync, only the segment delete can
        8.times { src.publish "x" * 100_000 }
        src_segment = File.join(queue_dir(s, "src"), "msgs.0000000001")
        wait_for { !File.exists?(src_segment) }
        sync = last_sync(s)
        sync.syncfs.should be_false
        sync.paths.should contain File.join(queue_dir(s, "dst"), "msgs.0000000001")
      end
    end
  end

  it "keeps readahead on a queue that only gets moved copies" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        ch.queue("dst")
        src = ch.queue("src", args: AMQP::Client::Arguments.new({
          "x-message-ttl"             => 1,
          "x-dead-letter-exchange"    => "",
          "x-dead-letter-routing-key" => "dst",
        }))
        src.publish "x"
        dst = s.vhosts["/"].queue("dst")
        wait_for { dst.message_count == 1 }
        dst.@msg_store.@synced_writes.should be_false
      end
    end
  end

  it "keeps the queue going while the unlink waits for the sync" do
    replicator = HeldSyncReplicator.new
    with_amqp_server(replicator: replicator) do |s|
      with_channel(s) do |ch|
        dst = ch.queue("dst")
        src = ch.queue("src", args: AMQP::Client::Arguments.new({
          "x-message-ttl"             => 1,
          "x-dead-letter-exchange"    => "",
          "x-dead-letter-routing-key" => "dst",
        }))
        replicator.hold
        # The first segment empties after a few of them, and its unlink
        # waits for a sync that can't finish until the followers are released
        8.times { src.publish "x" * 100_000 }
        vhost = s.vhosts["/"]
        wait_for { vhost.queue("dst").message_count == 8 }
        src.publish "x"
        wait_for { vhost.queue("dst").message_count == 9 }
        dst.get(no_ack: true).should_not be_nil
        src_segment = File.join(queue_dir(s, "src"), "msgs.0000000001")
        File.exists?(src_segment).should be_true
        replicator.release
        wait_for { !File.exists?(src_segment) }
      end
    ensure
      replicator.release
    end
  end

  it "unlinks at once when sync is disabled" do
    LavinMQ::Config.instance.sync = false
    with_amqp_server do |s|
      with_channel(s) do |ch|
        ch.queue("dst")
        src = ch.queue("src", args: AMQP::Client::Arguments.new({
          "x-message-ttl"             => 1,
          "x-dead-letter-exchange"    => "",
          "x-dead-letter-routing-key" => "dst",
        }))
        8.times { src.publish "x" * 100_000 }
        src_segment = File.join(queue_dir(s, "src"), "msgs.0000000001")
        wait_for { !File.exists?(src_segment) }
        s.persister.last_sync.should be_nil
      end
    end
  end
end

describe LavinMQ::MessageStore, "unlinks of segments that messages were moved out of" do
  it "waits for a sync after the store is loaded again" do
    with_datadir do |data_dir|
      replicator = HeldSyncReplicator.new
      persister = LavinMQ::Persister.new(data_dir, replicator)
      msg_dir = File.join(data_dir, "q")
      Dir.mkdir_p msg_dir
      begin
        sps = moved_one_and_reloaded(msg_dir, persister) do |store|
          replicator.hold
          store
        end
        store = sps[:store]
        sps[:rest].each { |sp| store.delete(sp) }
        segment = File.join(msg_dir, "msgs.0000000001")
        File.exists?(segment).should be_true
        replicator.release
        wait_for { !File.exists?(segment) }
        # Copies an earlier process made were never marked dirty
        persister.last_sync.should_not(be_nil).syncfs.should be_true
        store.close
      ensure
        replicator.release
        persister.close
      end
    end
  end

  it "deletes a store without waiting for the sync, and the pending unlink then leaves the path alone" do
    with_datadir do |data_dir|
      replicator = HeldSyncReplicator.new
      persister = LavinMQ::Persister.new(data_dir, replicator)
      msg_dir = File.join(data_dir, "q")
      Dir.mkdir_p msg_dir
      begin
        store = LavinMQ::MessageStore.new(msg_dir, nil, persister: persister)
        sps = push_and_shift(store)
        replicator.hold
        sps.select(&.segment.==(1)).each { |sp| store.delete(sp, moved: true) }
        segment = File.join(msg_dir, "msgs.0000000001")
        File.exists?(segment).should be_true
        store.delete # would wait for the held sync if it waited for the unlinks
        Dir.exists?(msg_dir).should be_false
        # A queue declared again with the same name gets the same paths
        Dir.mkdir_p msg_dir
        File.write(segment, "new")
        replicator.release
        persister.sync
        Fiber.yield
        File.exists?(segment).should be_true
      ensure
        replicator.release
        persister.close
      end
    end
  end
end

# 4 messages of a quarter segment each: 3 in segment 1, 1 in segment 2
private def push_and_shift(store) : Array(LavinMQ::SegmentPosition)
  body = "x" * (LavinMQ::Config.instance.segment_size // 4)
  4.times { store.push(LavinMQ::Message.new("", "rk", body)) }
  Array(LavinMQ::SegmentPosition).new(4) { store.shift?.should_not(be_nil).segment_position }
end

# Moves one message out of segment 1, then loads the store again
private def moved_one_and_reloaded(msg_dir, persister, &)
  store = LavinMQ::MessageStore.new(msg_dir, nil, persister: persister)
  sps = push_and_shift(store)
  first, *rest = sps.select(&.segment.==(1))
  store.delete(first, moved: true)
  store.close
  store = yield LavinMQ::MessageStore.new(msg_dir, nil, persister: persister)
  {store: store, rest: rest}
end

# Once held, every wait for the followers waits until released
private class HeldSyncReplicator < NoOpReplicator
  @held = Atomic(Bool).new(false)
  @released = Channel(Nil).new

  def wait_for_followers : Nil
    @released.receive? if @held.get
  end

  def hold : Nil
    @held.set(true)
  end

  def release : Nil
    @released.close
  end
end

describe LavinMQ::Persister, "segments emptied by plain acks" do
  it "unlinks the segment without a sync when nothing was moved out" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("plain")
        # Plain publishes and no_ack gets: nothing requests a sync
        8.times { q.publish "x" * 100_000 }
        8.times { q.get(no_ack: true).should_not be_nil }
        segment = File.join(queue_dir(s, "plain"), "msgs.0000000001")
        wait_for { !File.exists?(segment) }
        s.persister.last_sync.should be_nil
      end
    end
  end
end
