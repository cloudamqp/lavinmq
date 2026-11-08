require "./spec_helper"

private def queue_dir(s : LavinMQ::Server, queue_name : String) : String
  File.join(s.vhosts["/"].data_dir, Digest::SHA1.hexdigest(queue_name))
end

private def last_sync(s : LavinMQ::Server) : LavinMQ::Persister::SyncRecord
  s.persister.last_sync.should_not be_nil
end

# Rejects (requeue: true) a message the first time it's delivered, so it goes
# to the retry queue, and acks it when the retry queue releases it back
private def reject_once_into_retry_queue(q)
  q.subscribe(no_ack: false) do |m|
    if m.properties.headers.try &.has_key?("x-delivery-count")
      m.ack
    else
      m.reject(requeue: true)
    end
  end
end

# Declares "dst", and queue *name* that dead-letters into it, after *ttl* ms
private def declare_dead_lettering(ch, name = "src", ttl : Int32? = 1)
  ch.queue("dst")
  args = AMQP::Client::Arguments.new({
    "x-dead-letter-exchange"    => "",
    "x-dead-letter-routing-key" => "dst",
  })
  args["x-message-ttl"] = ttl if ttl
  ch.queue(name, args: args)
end

# Dead-letters two small messages out of queue *name* into "dst". They don't
# empty a segment, so nothing requests the drain that syncs the copies.
# Bytes written to the ack file of *segment* of queue *name*. The files are
# preallocated, so the size on disk doesn't tell.
private def acked_bytes(s : LavinMQ::Server, name : String, segment = 1u32) : Int64
  store = s.vhosts["/"].queue(name).as(LavinMQ::AMQP::Queue).@msg_store
  store.@acks[segment]?.try(&.size.to_i64) || 0i64
end

# Publishes from a connection of its own, and closes the returned channel when
# done. Once the expiry waits for a drain under the queue's lock, the server
# stops reading that connection, and the publishes that don't fit in the
# socket buffers (small on macOS) block the publisher until the drain runs.
private def publish_in_background(s : LavinMQ::Server, queue : String, count : Int, body : String) : Channel(Nil)
  published = Channel(Nil).new
  spawn do
    with_channel(s) do |ch|
      count.times { ch.basic_publish(body, "", queue) }
    end
  ensure
    published.close
  end
  published
end

# Publishes 40 messages into "src" (see #declare_dead_lettering) while the
# drains are held, five to a segment, so the expiry keeps more emptied segments
# than MAX_KEPT_SEGMENTS, and yields once it waits for a drain. Returns once
# every message is dead-lettered after the drains are released.
# Dead-letters 40 messages of 100 kB from "src" to "dst" while the drains
# are held, and yields while they're held
private def move_while_drains_held(s : LavinMQ::Server, &) : Nil
  with_channel(s) { |ch| declare_dead_lettering(ch) }
  dst = s.vhosts["/"].queue("dst")
  published = with_drain_held do
    publisher = publish_in_background(s, "src", 40, "x" * 100_000)
    # The expiry doesn't wait for the drain: it holds the acks back
    wait_for(timeout: 5.seconds) { dst.message_count == 40 }
    acked_bytes(s, "src").should eq 0
    yield
    publisher
  end
  published.receive?
end

# Whether a drain synced a file in *dir*, or the whole filesystem, from the
# drain recorded *since* syncs ago on
private def synced_in?(s : LavinMQ::Server, dir : String, since = 0) : Bool
  s.persister.syncs[since..].any? { |r| r.syncfs || r.paths.any?(&.starts_with?(dir)) }
end

# Waits until *path* is unlinked. A store unlinks an emptied segment whose
# messages it copied out once their drain is done, at its next delete or
# rollover (see MessageStore#unlink_segment), which the block causes.
private def wait_unlinked(path : String, &) : Nil
  wait_for do
    next true unless File.exists?(path)
    yield
    sleep 20.milliseconds # each call publishes
    false
  end
end

# Spec instrumentation: fails the unlink of *path*, like the syscall would,
# after MFile#delete marked the file deleted
class MFile
  class_property fail_delete_of : String? = nil
end

class File
  def self.delete(path : Path | String) : Nil
    raise File::Error.new("Spec unlink failure", file: path.to_s) if MFile.fail_delete_of == path.to_s
    previous_def
  end

  def self.delete?(path : Path | String) : Bool
    raise File::Error.new("Spec unlink failure", file: path.to_s) if MFile.fail_delete_of == path.to_s
    previous_def
  end
end

# A store with three messages to a segment, whose first two segments are full
# and the third holds one message
private def store_with_three_segments(data_dir, persister) : LavinMQ::MessageStore
  store = LavinMQ::MessageStore.new(data_dir, nil, persister: persister)
  body = "x" * (LavinMQ::Config.instance.segment_size // 4)
  7.times { store.push(LavinMQ::Message.new("", "rk", body)) }
  store.@segments.keys.should eq [1u32, 2u32, 3u32]
  store
end

# Waits for a drain that takes everything marked so far, so a spec can check
# what was or wasn't marked without waiting for a time
private def drain_marked(persister : LavinMQ::Persister) : Nil
  persister.wait_drain(persister.copy_drain)
end

# A leader's replicator with one in-sync follower, recording the paths the
# drains ask followers to fsync
private class OneFollowerReplicator < NoOpReplicator
  getter socket : FakeSocket
  getter peer : UNIXSocket
  @follower : LavinMQ::Clustering::Follower
  @fsync_requests = Array(String).new
  @lock = Mutex.new

  def initialize(data_dir : String)
    @socket, @peer = FakeSocket.pair
    @follower = LavinMQ::Clustering::Follower.new(@socket, data_dir, FakeFileIndex.new(data_dir))
    @follower.mark_synced!
  end

  def followers : Array(LavinMQ::Clustering::Follower)
    [@follower]
  end

  def request_fsync(paths : Enumerable(String)) : Nil
    @lock.synchronize { @fsync_requests.concat(paths) }
  end

  def fsync_requested?(path : String) : Bool
    @lock.synchronize { @fsync_requests.includes?(path) }
  end

  def close
    @socket.close
    @peer.close
  end
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
        persister.drains.should eq 0
      ensure
        store.close
        persister.close
      end
    end
  end

  it "syncs a finished queue segment that a confirmed publish wrote to" do
    with_datadir do |data_dir|
      persister = LavinMQ::Persister.new(data_dir)
      store = LavinMQ::MessageStore.new(data_dir, nil, persister: persister)
      receiver = ConfirmReceiver.new
      begin
        body = "x" * (LavinMQ::Config.instance.segment_size // 4)
        confirmed = LavinMQ::Message.new("", "rk", body)
        confirmed.needs_sync = true
        store.push(confirmed)
        persister.enqueue_ack(receiver, 1u64)
        receiver.confirms.receive.should eq 1u64
        # Not confirmed, so only the rollover can sync the rest of msgs.1
        4.times { store.push(LavinMQ::Message.new("", "rk", body)) }
        store.@segments.size.should eq 2
        meta = File.join(data_dir, "meta.0000000001")
        sync = wait_for { persister.last_sync.try { |r| r if r.paths.includes?(meta) } }
        sync.paths.should contain File.join(data_dir, "msgs.0000000001")
      ensure
        store.close
        persister.close
      end
    end
  end

  it "syncs a finished queue segment that a moved copy wrote to" do
    with_datadir do |data_dir|
      persister = LavinMQ::Persister.new(data_dir)
      store = LavinMQ::MessageStore.new(data_dir, nil, persister: persister)
      begin
        body = "x" * (LavinMQ::Config.instance.segment_size // 4)
        moved = LavinMQ::Message.new("", "rk", body)
        moved.moved = true
        store.push(moved)
        4.times { store.push(LavinMQ::Message.new("", "rk", body)) }
        store.@segments.size.should eq 2
        meta = File.join(data_dir, "meta.0000000001")
        sync = wait_for { persister.last_sync.try { |r| r if r.paths.includes?(meta) } }
        sync.paths.should contain File.join(data_dir, "msgs.0000000001")
      ensure
        store.close
        persister.close
      end
    end
  end

  it "leaves a finished queue segment to the drain when its last message was confirmed" do
    with_datadir do |data_dir|
      persister = LavinMQ::Persister.new(data_dir)
      store = LavinMQ::MessageStore.new(data_dir, nil, persister: persister)
      receiver = ConfirmReceiver.new
      begin
        body = "x" * (LavinMQ::Config.instance.segment_size // 4)
        3.times do
          confirmed = LavinMQ::Message.new("", "rk", body)
          confirmed.needs_sync = true
          store.push(confirmed)
        end
        persister.enqueue_ack(receiver, 1u64)
        receiver.confirms.receive.should eq 1u64
        # The confirm's drain synced all of msgs.1, so the rollover doesn't
        store.push(LavinMQ::Message.new("", "rk", body))
        store.@segments.size.should eq 2
        drain_marked(persister)
        persister.syncs.none?(&.paths.includes?(File.join(data_dir, "meta.0000000001"))).should be_true
      ensure
        store.close
        persister.close
      end
    end
  end

  it "syncs a finished queue segment that a syncfs for another queue ran during" do
    with_datadir do |data_dir|
      persister = LavinMQ::Persister.new(data_dir)
      store = LavinMQ::MessageStore.new(data_dir, nil, persister: persister)
      begin
        body = "x" * (LavinMQ::Config.instance.segment_size // 4)
        store.push(LavinMQ::Message.new("", "rk", body))
        # A transaction commit on another queue syncs the whole filesystem
        persister.sync
        persister.last_sync.should eq LavinMQ::Persister::SyncRecord.new(true, [] of String)
        4.times { store.push(LavinMQ::Message.new("", "rk", body)) }
        store.@segments.size.should eq 2
        meta = File.join(data_dir, "meta.0000000001")
        sync = wait_for { persister.last_sync.try { |r| r if r.paths.includes?(meta) } }
        sync.paths.should contain File.join(data_dir, "msgs.0000000001")
      ensure
        store.close
        persister.close
      end
    end
  end

  it "syncs a finished queue segment, here and on the followers, when the node has followers" do
    with_datadir do |data_dir|
      replicator = OneFollowerReplicator.new(data_dir)
      persister = LavinMQ::Persister.new(data_dir, replicator)
      store = LavinMQ::MessageStore.new(data_dir, replicator, persister: persister)
      begin
        body = "x" * (LavinMQ::Config.instance.segment_size // 4)
        # Nothing on this node syncs msgs.1, but a follower's own syncfs may
        # have caught part of a message in it
        4.times { store.push(LavinMQ::Message.new("", "rk", body)) }
        store.@segments.size.should eq 2
        meta = File.join(data_dir, "meta.0000000001")
        msgs = File.join(data_dir, "msgs.0000000001")
        sync = wait_for { persister.last_sync.try { |r| r if r.paths.includes?(meta) } }
        sync.paths.should contain msgs
        replicator.fsync_requested?(msgs).should be_true
        replicator.fsync_requested?(meta).should be_true
      ensure
        store.close
        persister.close
        replicator.close
      end
    end
  end

  it "leaves a finished queue segment to the kernel when the node has no followers" do
    with_datadir do |data_dir|
      replicator = NoOpReplicator.new
      persister = LavinMQ::Persister.new(data_dir, replicator)
      store = LavinMQ::MessageStore.new(data_dir, replicator, persister: persister)
      begin
        body = "x" * (LavinMQ::Config.instance.segment_size // 4)
        4.times { store.push(LavinMQ::Message.new("", "rk", body)) }
        store.@segments.size.should eq 2
        drain_marked(persister)
        persister.syncs.none?(&.paths.includes?(File.join(data_dir, "meta.0000000001"))).should be_true
      ensure
        store.close
        persister.close
      end
    end
  end

  it "leaves a finished queue segment to the kernel when a syncfs finished before it was opened" do
    with_datadir do |data_dir|
      persister = LavinMQ::Persister.new(data_dir)
      persister.sync
      store = LavinMQ::MessageStore.new(data_dir, nil, persister: persister)
      begin
        body = "x" * (LavinMQ::Config.instance.segment_size // 4)
        4.times { store.push(LavinMQ::Message.new("", "rk", body)) }
        store.@segments.size.should eq 2
        drain_marked(persister)
        persister.syncs.none?(&.paths.includes?(File.join(data_dir, "meta.0000000001"))).should be_true
      ensure
        store.close
        persister.close
      end
    end
  end

  it "leaves a later finished segment to the kernel when nothing synced it while it was written" do
    with_datadir do |data_dir|
      persister = LavinMQ::Persister.new(data_dir)
      store = LavinMQ::MessageStore.new(data_dir, nil, persister: persister)
      receiver = ConfirmReceiver.new
      begin
        body = "x" * (LavinMQ::Config.instance.segment_size // 4)
        confirmed = LavinMQ::Message.new("", "rk", body)
        confirmed.needs_sync = true
        store.push(confirmed)
        persister.enqueue_ack(receiver, 1u64)
        receiver.confirms.receive.should eq 1u64
        # Three to a segment: fills msgs.1, then msgs.2, and opens msgs.3
        6.times { store.push(LavinMQ::Message.new("", "rk", body)) }
        store.@segments.size.should eq 3
        wait_for { persister.last_sync.try(&.paths.includes?(File.join(data_dir, "meta.0000000001"))) }
        drain_marked(persister)
        persister.syncs.none?(&.paths.includes?(File.join(data_dir, "meta.0000000002"))).should be_true
      ensure
        store.close
        persister.close
      end
    end
  end

  it "leaves an emptied finished segment to its unlink" do
    with_datadir do |data_dir|
      persister = LavinMQ::Persister.new(data_dir)
      store = LavinMQ::MessageStore.new(data_dir, nil, persister: persister)
      begin
        body = "x" * (LavinMQ::Config.instance.segment_size // 4)
        3.times { store.push(LavinMQ::Message.new("", "rk", body)) }
        persister.sync # marks msgs.1 as partly synced
        3.times do
          sp = store.shift?.should_not(be_nil).segment_position
          store.copied_out(sp)
          store.delete(sp)
        end
        # The rollover finds msgs.1 emptied, and keeps it until its copies sync
        store.push(LavinMQ::Message.new("", "rk", body))
        store.kept_segment_drain.should_not be_nil
        drain_marked(persister)
        persister.syncs.none?(&.paths.includes?(File.join(data_dir, "meta.0000000001"))).should be_true
      ensure
        store.close
        persister.close
      end
    end
  end

  it "syncs a partly synced segment that is being written when the store closes" do
    with_datadir do |data_dir|
      persister = LavinMQ::Persister.new(data_dir)
      store = LavinMQ::MessageStore.new(data_dir, nil, persister: persister)
      begin
        store.push(LavinMQ::Message.new("", "rk", "a"))
        persister.sync # may catch a message being written to msgs.1
        store.push(LavinMQ::Message.new("", "rk", "b"))
        store.close
        msgs = File.join(data_dir, "msgs.0000000001")
        wait_for { persister.last_sync.try(&.paths.includes?(msgs)) }
      ensure
        store.close
        persister.close
      end
    end
  end

  it "syncs a partly synced finished segment after sync is disabled during its rollover" do
    with_datadir do |data_dir|
      persister = LavinMQ::Persister.new(data_dir)
      store = LavinMQ::MessageStore.new(data_dir, nil, persister: persister)
      begin
        body = "x" * (LavinMQ::Config.instance.segment_size // 4)
        store.push(LavinMQ::Message.new("", "rk", body))
        persister.sync
        synced_before = persister.syncs.size
        LavinMQ::Config.instance.sync = false
        3.times { store.push(LavinMQ::Message.new("", "rk", body)) }
        store.@segments.size.should eq 2
        LavinMQ::Config.instance.sync = true
        drain_marked(persister)
        meta = File.join(data_dir, "meta.0000000001")
        persister.syncs[synced_before..].any? { |r| r.syncfs || r.paths.includes?(meta) }.should be_true
      ensure
        LavinMQ::Config.instance.sync = true
        store.close
        persister.close
      end
    end
  end

  it "doesn't let finished segment syncs add up to a syncfs" do
    with_datadir do |data_dir|
      threshold = LavinMQ::Config.instance.syncfs_threshold
      LavinMQ::Config.instance.syncfs_threshold = 2
      persister = LavinMQ::Persister.new(data_dir)
      segments = Array.new(3) do |i|
        MFile.new(File.join(data_dir, "msgs.000000000#{i + 1}"), 4096).tap(&.write_bytes(1u32))
      end
      begin
        segments.each { |f| persister.sync_soon(f, File.join(data_dir, "meta.#{f.path[-10..]}")) }
        drain_marked(persister)
        syncs = persister.syncs
        syncs.should_not be_empty
        syncs.none?(&.syncfs).should be_true
        syncs.all? { |r| r.paths.count(data_dir) <= 1 }.should be_true
      ensure
        LavinMQ::Config.instance.syncfs_threshold = threshold
        segments.each &.close
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
  it "asks followers for the files a drain took, not a syncfs, while sync is disabled" do
    replicator = SyncfsCountingReplicator.new
    with_amqp_server(replicator: replicator) do |s|
      LavinMQ::Config.instance.sync = false
      with_channel(s) do |ch|
        q = ch.queue("confirmed")
        3.times { q.publish_confirm("x").should be_true }
      end
      replicator.syncfs_requests.get.should eq 0
    end
  end

  it "asks followers for a syncfs after sync is re-enabled, as they skipped their fsyncs too" do
    replicator = SyncfsCountingReplicator.new
    with_amqp_server(replicator: replicator) do |s|
      with_channel(s) do |ch|
        q = ch.queue("confirmed")
        LavinMQ::Config.instance.sync = false
        q.publish_confirm("x").should be_true
        requests = replicator.syncfs_requests.get
        LavinMQ::Config.instance.sync = true
        q.publish_confirm("y").should be_true
        replicator.syncfs_requests.get.should eq requests + 1
        q.publish_confirm("z").should be_true
        replicator.syncfs_requests.get.should eq requests + 1
      end
    end
  end
end

private class SyncfsCountingReplicator < NoOpReplicator
  getter syncfs_requests = Atomic(Int32).new(0)

  def request_syncfs : Nil
    @syncfs_requests.add(1)
  end
end

describe LavinMQ::Persister, "messages the broker moves between queues" do
  it "keeps a segment until the drain that syncs its dead-lettered copies has run, without a syncfs" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        src = declare_dead_lettering(ch)
        src_segment = File.join(queue_dir(s, "src"), "msgs.0000000001")
        with_drain_held do
          # Plain publishes: no confirm requests a sync, only the copies can
          8.times { src.publish "x" * 100_000 }
          wait_for { s.vhosts["/"].queue("dst").message_count == 8 }
          sleep 50.milliseconds # the sources are deleted right after their copies
          File.exists?(src_segment).should be_true
        end
        wait_unlinked(src_segment) { src.publish "x" }
        synced_in?(s, queue_dir(s, "dst")).should be_true
        s.persister.syncs.none?(&.syncfs).should be_true
      end
    end
  end

  it "waits for the drain that syncs the copies before a purge unlinks a segment they were moved out of" do
    with_datadir do |data_dir|
      persister = LavinMQ::Persister.new(data_dir)
      store = store_with_three_segments(data_dir, persister)
      segment = File.join(data_dir, "msgs.0000000001")
      begin
        moved = store.shift?.should_not(be_nil).segment_position
        store.shift? # stays unacked, so the move doesn't empty segment 1
        store.shift?
        store.shift? # the reader moves on to segment 2
        purged = Channel(Nil).new
        with_drain_held do
          store.copied_out(moved)
          store.delete(moved)
          spawn { store.purge_all; purged.close }
          sleep 50.milliseconds
          File.exists?(segment).should be_true
        end
        purged.receive?
        File.exists?(segment).should be_false
        store.copies_synced?.should be_true
      ensure
        store.close
        persister.close
      end
    end
  end

  it "unlinks an emptied segment before its acks, so a crash in between doesn't bring its messages back" do
    with_datadir do |data_dir|
      store = store_with_three_segments(data_dir, nil)
      begin
        sps = Array.new(3) { store.shift?.should_not(be_nil).segment_position }
        # Like a crash between the two unlinks
        MFile.fail_delete_of = File.join(data_dir, "msgs.0000000001")
        sps.first(2).each { |sp| store.delete(sp) }
        expect_raises(LavinMQ::MessageStore::Error) { store.delete(sps[2]) }
      ensure
        MFile.fail_delete_of = nil
        store.close
      end
      reloaded = LavinMQ::MessageStore.new(data_dir, nil)
      begin
        reloaded.size.should eq 4
      ensure
        reloaded.close
      end
    end
  end

  it "writes many held back acks in one pass without recursing per ack" do
    with_datadir do |data_dir|
      persister = LavinMQ::Persister.new(data_dir)
      store = LavinMQ::MessageStore.new(data_dir, nil, persister: persister)
      count = 60_000
      begin
        count.times { store.push(LavinMQ::Message.new("", "rk", "x")) }
        with_drain_held do
          count.times do
            sp = store.shift?.should_not(be_nil).segment_position
            store.copied_out(sp)
            store.delete(sp)
          end
        end
        wait_for { store.copies_synced? }
        # Writes every held back ack: a nested call per ack would overflow the stack
        store.unlink_synced_segments
        store.kept_segment_drain.should be_nil
      ensure
        store.close
        persister.close
      end
    end
  end

  it "keeps an emptied segment whose unlink failed, and raises it as a store error" do
    with_datadir do |data_dir|
      persister = LavinMQ::Persister.new(data_dir)
      store = store_with_three_segments(data_dir, persister)
      segment = File.join(data_dir, "msgs.0000000001")
      begin
        sps = Array.new(4) { store.shift?.should_not(be_nil).segment_position }
        with_drain_held do
          sps.first(3).each do |sp|
            store.copied_out(sp)
            store.delete(sp)
          end
        end
        wait_for { store.copies_synced? }
        File.exists?(segment).should be_true # kept until the next delete
        MFile.fail_delete_of = segment
        expect_raises(LavinMQ::MessageStore::Error) { store.delete(sps[3]) }
        File.exists?(segment).should be_true
        File.exists?(File.join(data_dir, "acks.0000000001")).should be_true
        MFile.fail_delete_of = nil
        store.purge(1)
        File.exists?(segment).should be_false
        File.exists?(File.join(data_dir, "acks.0000000001")).should be_false
      ensure
        MFile.fail_delete_of = nil
        store.close
        persister.close
      end
    end
  end

  it "unlinks a kept segment once its drain is done, when nothing else happens on the queue" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        src = declare_dead_lettering(ch)
        src_segment = File.join(queue_dir(s, "src"), "msgs.0000000001")
        with_drain_held do
          8.times { src.publish "x" * 100_000 }
          wait_for { s.vhosts["/"].queue("dst").message_count == 8 }
          sleep 50.milliseconds
          File.exists?(src_segment).should be_true
        end
        # No publish or delete on src from here
        wait_for { !File.exists?(src_segment) }
        synced_in?(s, queue_dir(s, "dst")).should be_true
      end
    end
  end

  it "unlinks a kept priority queue segment once its drain is done, when nothing else happens on its priority" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        ch.queue("dst")
        src = ch.queue("src", args: AMQP::Client::Arguments.new({
          "x-max-priority"            => 1,
          "x-message-ttl"             => 1,
          "x-dead-letter-exchange"    => "",
          "x-dead-letter-routing-key" => "dst",
        }))
        src_segment = File.join(queue_dir(s, "src"), "prio.001", "msgs.0000000001")
        props = AMQP::Client::Properties.new(priority: 1u8)
        with_drain_held do
          8.times { src.publish "x" * 100_000, props: props }
          wait_for { s.vhosts["/"].queue("dst").message_count == 8 }
          sleep 50.milliseconds
          File.exists?(src_segment).should be_true
        end
        wait_for { !File.exists?(src_segment) }
      end
    end
  end

  it "doesn't make a queue wait for a held drain, and unlinks its segments after it" do
    with_amqp_server do |s|
      move_while_drains_held(s) { }
      src_segment = File.join(queue_dir(s, "src"), "msgs.0000000001")
      wait_for(timeout: 5.seconds) { !File.exists?(src_segment) }
      synced_in?(s, queue_dir(s, "dst")).should be_true
    end
  end

  it "writes the held back acks when sync is disabled while the drain is held" do
    with_amqp_server do |s|
      move_while_drains_held(s) do
        # A SIGHUP reload can turn sync off: the drains that follow skip
        # their sync, and must not leave the acks held back
        LavinMQ::Config.instance.sync = false
      end
      src_segment = File.join(queue_dir(s, "src"), "msgs.0000000001")
      wait_for(timeout: 5.seconds) { !File.exists?(src_segment) }
    end
  end

  it "keeps a priority queue segment until the drain that syncs its copies has run" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        ch.queue("dst")
        src = ch.queue("src", args: AMQP::Client::Arguments.new({
          "x-max-priority"            => 1,
          "x-message-ttl"             => 1,
          "x-dead-letter-exchange"    => "",
          "x-dead-letter-routing-key" => "dst",
        }))
        src_segment = File.join(queue_dir(s, "src"), "prio.000", "msgs.0000000001")
        with_drain_held do
          8.times { src.publish "x" * 100_000 }
          wait_for { s.vhosts["/"].queue("dst").message_count == 8 }
          sleep 50.milliseconds
          File.exists?(src_segment).should be_true
        end
        wait_unlinked(src_segment) { src.publish "x" }
        synced_in?(s, queue_dir(s, "dst")).should be_true
      end
    end
  end

  it "keeps a delayed exchange segment until the drain that syncs the released messages has run" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        x = ch.exchange("delayed", "direct", args: AMQP::Client::Arguments.new({"x-delayed-exchange" => true}))
        ch.queue("dst").bind(x.name, "dst")
        props = AMQ::Protocol::Properties.new(headers: AMQP::Client::Arguments.new({"x-delay" => 1}))
        delayed_segment = File.join(queue_dir(s, "amq.delayed-delayed"), "msgs.0000000001")
        with_drain_held do
          8.times { x.publish "x" * 100_000, "dst", props: props }
          wait_for { s.vhosts["/"].queue("dst").message_count == 8 }
          sleep 50.milliseconds
          File.exists?(delayed_segment).should be_true
        end
        wait_unlinked(delayed_segment) { x.publish "x", "dst", props: props }
        synced_in?(s, queue_dir(s, "dst")).should be_true
      end
    end
  end

  it "keeps a segment until the drain that syncs the messages rejected into the retry queue has run" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("prim", args: AMQP::Client::Arguments.new({"x-delayed-retry-min" => 60_000}))
        prim_segment = File.join(queue_dir(s, "prim"), "msgs.0000000001")
        with_drain_held do
          reject_once_into_retry_queue(q)
          8.times { q.publish "x" * 100_000 }
          wait_for { s.vhosts["/"].queue("amq.retry-prim").message_count == 8 }
          sleep 50.milliseconds
          File.exists?(prim_segment).should be_true
        end
        wait_unlinked(prim_segment) { q.publish "x" }
        synced_in?(s, queue_dir(s, "amq.retry-prim")).should be_true
      end
    end
  end

  it "keeps a retry queue segment until the drain that syncs the messages it released has run" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("prim", args: AMQP::Client::Arguments.new({"x-delayed-retry-min" => 500}))
        retry_segment = File.join(queue_dir(s, "amq.retry-prim"), "msgs.0000000001")
        with_drain_held do
          reject_once_into_retry_queue(q)
          8.times { q.publish "x" * 100_000 }
          retry_q = s.vhosts["/"].queue("amq.retry-prim")
          wait_for { retry_q.message_count == 8 }
          wait_for(timeout: 5.seconds) { retry_q.message_count == 0 }
          sleep 50.milliseconds
          File.exists?(retry_segment).should be_true
        end
        wait_unlinked(retry_segment) { q.publish "x" }
        synced_in?(s, queue_dir(s, "prim")).should be_true
      end
    end
  end

  it "keeps a retry queue segment until the drain that syncs the messages that expired out of it has run" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        ch.queue("dst")
        q = ch.queue("prim", args: AMQP::Client::Arguments.new({
          "x-delayed-retry-min"       => 60_000,
          "x-message-ttl"             => 500,
          "x-dead-letter-exchange"    => "",
          "x-dead-letter-routing-key" => "dst",
        }))
        retry_segment = File.join(queue_dir(s, "amq.retry-prim"), "msgs.0000000001")
        with_drain_held do
          reject_once_into_retry_queue(q)
          8.times { q.publish "x" * 100_000 }
          wait_for(timeout: 5.seconds) { s.vhosts["/"].queue("dst").message_count == 8 }
          sleep 50.milliseconds
          File.exists?(retry_segment).should be_true
        end
        wait_unlinked(retry_segment) { q.publish "x" }
        synced_in?(s, queue_dir(s, "dst")).should be_true
      end
    end
  end

  it "doesn't sync before an unlink when the dead-letter exchange routes nowhere" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        src = ch.queue("src", args: AMQP::Client::Arguments.new({
          "x-message-ttl"             => 1,
          "x-dead-letter-exchange"    => "",
          "x-dead-letter-routing-key" => "missing",
        }))
        8.times { src.publish "x" * 100_000 }
        wait_for { !File.exists?(File.join(queue_dir(s, "src"), "msgs.0000000001")) }
        s.persister.last_sync.should be_nil
      end
    end
  end

  it "doesn't sync before an unlink when messages were dead-lettered only into a transient queue" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        ch.queue("transient", durable: false)
        src = ch.queue("src", args: AMQP::Client::Arguments.new({
          "x-message-ttl"             => 1,
          "x-dead-letter-exchange"    => "",
          "x-dead-letter-routing-key" => "transient",
        }))
        8.times { src.publish "x" * 100_000 }
        wait_for { !File.exists?(File.join(queue_dir(s, "src"), "msgs.0000000001")) }
        s.persister.last_sync.should be_nil
      end
    end
  end

  it "syncs after sync is re-enabled for messages moved while it was disabled" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        src = declare_dead_lettering(ch, ttl: nil)
        dead_letter = true
        src.subscribe(no_ack: false) { |m| dead_letter ? m.reject : m.ack }
        LavinMQ::Config.instance.sync = false
        # Five messages fill segment 1, and all of them are dead-lettered
        5.times { src.publish "x" * 100_000 }
        wait_for { s.vhosts["/"].queue("dst").message_count == 5 }
        # A confirm makes a drain run while sync is disabled: it takes the
        # copies' marks without syncing them. On a channel of its own, so the
        # publishes below aren't confirmed.
        with_channel(s, &.queue("confirmed").publish_confirm("x"))
        drains = s.persister.drains
        # From here on only plain acks: the rollover unlinks segment 1 while
        # sync is disabled, so only the earlier copies can call for a sync
        dead_letter = false
        src.publish "x" * 100_000
        wait_for { !File.exists?(File.join(queue_dir(s, "src"), "msgs.0000000001")) }
        s.persister.drains.should eq drains
        LavinMQ::Config.instance.sync = true
        5.times { src.publish "x" * 100_000 }
        wait_unlinked(File.join(queue_dir(s, "src"), "msgs.0000000002")) { src.publish "x" }
        # The drains that took the copies skipped their sync, so it's a syncfs
        s.persister.syncs.any?(&.syncfs).should be_true
      end
    end
  end

  it "holds back the ack of a moved message until the drain that syncs its copy is done" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        src = declare_dead_lettering(ch)
        with_drain_held do
          3.times { src.publish "x" }
          wait_for { s.vhosts["/"].queue("dst").message_count == 3 }
          sleep 50.milliseconds
          acked_bytes(s, "src").should eq 0
        end
        # The moves request the drain, nothing else happens on the queue
        wait_for { acked_bytes(s, "src") == 3 * sizeof(UInt32) }
        synced_in?(s, queue_dir(s, "dst")).should be_true
      end
    end
  end

  it "syncs the held back acks once they're written" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        src = declare_dead_lettering(ch)
        3.times { src.publish "x" }
      end
      acks = File.join(queue_dir(s, "src"), "acks.0000000001")
      wait_for { acked_bytes(s, "src") == 3 * sizeof(UInt32) }
      wait_for { s.persister.syncs.any? { |r| r.syncfs || r.paths.includes?(acks) } }
    end
  end

  it "keeps the readahead of queues that moved messages are written to" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        src = declare_dead_lettering(ch)
        q = ch.queue("prim", args: AMQP::Client::Arguments.new({"x-delayed-retry-min" => 1}))
        reject_once_into_retry_queue(q)
        8.times { src.publish "x" * 100_000 }
        q.publish "x"
        vhost = s.vhosts["/"]
        wait_for { vhost.queue("dst").message_count == 8 }
        wait_for { vhost.queue("prim").message_count == 0 && vhost.queue("amq.retry-prim").message_count == 0 }
        wait_for { s.persister.syncs.size > 0 } # the copies were synced
        {"dst", "prim", "amq.retry-prim"}.each do |name|
          vhost.queue(name).as(LavinMQ::AMQP::Queue).@msg_store.@synced_writes.should be_false
        end
      end
    end
  end

  it "keeps a segment until the drain that syncs the messages dead-lettered into a stream has run" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        ch.queue("dst", args: AMQP::Client::Arguments.new({"x-queue-type" => "stream"}))
        src = ch.queue("src", args: AMQP::Client::Arguments.new({
          "x-message-ttl"             => 1,
          "x-dead-letter-exchange"    => "",
          "x-dead-letter-routing-key" => "dst",
        }))
        src_segment = File.join(queue_dir(s, "src"), "msgs.0000000001")
        with_drain_held do
          8.times { src.publish "x" * 100_000 }
          wait_for { s.vhosts["/"].queue("dst").message_count == 8 }
          sleep 50.milliseconds
          File.exists?(src_segment).should be_true
        end
        wait_unlinked(src_segment) { src.publish "x" }
        synced_in?(s, queue_dir(s, "dst")).should be_true
        s.vhosts["/"].queue("dst").as(LavinMQ::AMQP::Stream).@msg_store.@synced_writes.should be_false
      end
    end
  end

  it "doesn't sync before unlinking a delayed exchange segment whose messages routed nowhere" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        x = ch.exchange("delayed", "direct", args: AMQP::Client::Arguments.new({"x-delayed-exchange" => true}))
        props = AMQ::Protocol::Properties.new(headers: AMQP::Client::Arguments.new({"x-delay" => 1}))
        8.times { x.publish "x" * 100_000, "unbound", props: props }
        wait_for { !File.exists?(File.join(queue_dir(s, "amq.delayed-delayed"), "msgs.0000000001")) }
        s.persister.last_sync.should be_nil
      end
    end
  end

  it "syncs the data dir once before the stores load" do
    with_amqp_server do |s|
      s.persister.local_syncs.should eq 1
      restart_server(s)
      s.persister.local_syncs.should eq 1
      LavinMQ::Config.instance.sync = false
      restart_server(s)
      s.persister.local_syncs.should eq 0
    end
  end

  it "deletes the segments a store kept at shutdown at the next start, after its sync" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        declare_dead_lettering(ch)
      end
      src_segment = File.join(queue_dir(s, "src"), "msgs.0000000001")
      with_drain_held do
        with_channel(s) do |ch|
          8.times { ch.queue("src", passive: true).publish "x" * 100_000 }
        end
        wait_for { s.vhosts["/"].queue("dst").message_count == 8 }
        sleep 50.milliseconds
        File.exists?(src_segment).should be_true
        restart_server(s)
      end
      s.persister.local_syncs.should eq 1
      File.exists?(src_segment).should be_false
      s.vhosts["/"].queue("dst").message_count.should eq 8
    end
  end

  it "writes the acks of a queue's moved messages before it closes, so a restart doesn't move them again" do
    with_amqp_server do |s|
      synced = s.persister.syncs.size
      queue = nil
      closed = Channel(Nil).new
      with_drain_held do
        with_channel(s) do |ch|
          q = declare_dead_lettering(ch, "restarted")
          2.times { q.publish "x" }
        end
        wait_for { s.vhosts["/"].queue("dst").message_count == 2 }
        queue = s.vhosts["/"].queue("restarted").as(LavinMQ::AMQP::Queue)
        spawn { queue.try &.close; closed.close }
        sleep 50.milliseconds
        queue.try(&.closed?).should be_true # closing, it waits for the drain
        acked_bytes(s, "restarted").should eq 0
      end
      closed.receive?
      synced_in?(s, queue_dir(s, "dst"), synced).should be_true
      s.persister.syncs[synced..].none?(&.syncfs).should be_true
      queue.should_not(be_nil).restart!.should be_true
      sleep 50.milliseconds
      s.vhosts["/"].queue("restarted").message_count.should eq 0
      s.vhosts["/"].queue("dst").message_count.should eq 2
    end
  end

  it "doesn't move the messages of a restarted priority queue again, when a sub-store moved them" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        ch.queue("dst")
        q = ch.queue("restarted", args: AMQP::Client::Arguments.new({
          "x-max-priority"            => 1,
          "x-message-ttl"             => 1,
          "x-dead-letter-exchange"    => "",
          "x-dead-letter-routing-key" => "dst",
        }))
        2.times { q.publish "x", props: AMQP::Client::Properties.new(priority: 1u8) }
      end
      wait_for { s.vhosts["/"].queue("dst").message_count == 2 }
      queue = s.vhosts["/"].queue("restarted").as(LavinMQ::AMQP::Queue)
      queue.close
      queue.restart!.should be_true
      sleep 50.milliseconds
      s.vhosts["/"].queue("restarted").message_count.should eq 0
      s.vhosts["/"].queue("dst").message_count.should eq 2
      synced_in?(s, queue_dir(s, "dst")).should be_true
    end
  end

  it "doesn't sync before a restarted queue reloads its store, when it has no unsynced copies" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        ch.queue("restarted").publish "x"
      end
      queue = s.vhosts["/"].queue("restarted").as(LavinMQ::AMQP::Queue)
      queue.close
      drains = s.persister.drains
      queue.restart!.should be_true
      s.persister.drains.should eq drains
    end
  end

  it "syncs the copies a retry queue released before it closes, and recreates it after" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("prim", args: AMQP::Client::Arguments.new({"x-delayed-retry-min" => 1}))
        reject_once_into_retry_queue(q)
        prim = s.vhosts["/"].queue("prim").as(LavinMQ::AMQP::Queue)
        retry_queue = s.vhosts["/"].queue("amq.retry-prim").as(LavinMQ::AMQP::Queue)
        synced = s.persister.syncs.size
        closed = Channel(Nil).new
        with_drain_held do
          q.publish "x" # released back to prim, and acked there
          wait_for { prim.@ack_count.get == 1 }
          spawn { retry_queue.close; closed.close }
          sleep 50.milliseconds
          # Closing, it waits for the drain that syncs the copy in prim
          acked_bytes(s, "amq.retry-prim").should eq 0
        end
        closed.receive?
        synced_in?(s, queue_dir(s, "prim"), synced).should be_true
        q.publish "y" # rejected into the retry queue, which is recreated
        wait_for { prim.@ack_count.get == 2 }
        s.vhosts["/"].queue("amq.retry-prim").should_not be retry_queue
      end
    end
  end

  it "deletes a queue while the retry queue it recreates waits for its sync" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("prim", args: AMQP::Client::Arguments.new({"x-delayed-retry-min" => 1}))
        reject_once_into_retry_queue(q)
        prim = s.vhosts["/"].queue("prim").as(LavinMQ::AMQP::Queue)
        q.publish "x" # released back to prim, unsynced
        wait_for { prim.@ack_count.get == 1 }
        s.vhosts["/"].queue("amq.retry-prim").as(LavinMQ::AMQP::Queue).close
        deleted = Channel(Nil).new
        with_drain_held do
          q.publish "y" # rejected, the retry queue's recreation waits for the drain
          sleep 50.milliseconds
          # The delete holds the definitions lock while it takes prim's retry queue lock
          spawn { s.vhosts["/"].delete_queue("prim"); deleted.close }
          sleep 50.milliseconds
        end
        select
        when deleted.receive?
        when timeout(5.seconds)
          fail "the queue delete deadlocked with the retry queue's recreation"
        end
        s.vhosts["/"].declare_queue("after", true, false)
        s.vhosts["/"].queue?("after").should_not be_nil
      end
    end
  end

  it "deletes a queue directly while the retry queue it recreates waits for the definitions lock" do
    with_amqp_server do |s|
      vhost = s.vhosts["/"]
      with_channel(s) do |ch|
        q = ch.queue("prim", args: AMQP::Client::Arguments.new({"x-delayed-retry-min" => 1}))
        reject_once_into_retry_queue(q)
        prim = vhost.queue("prim").as(LavinMQ::AMQP::Queue)
        q.publish "x"
        wait_for { prim.@ack_count.get == 1 }
        vhost.queue("amq.retry-prim").as(LavinMQ::AMQP::Queue).close
        deleted = Channel(Nil).new
        vhost.synchronize_definitions do
          q.publish "y" # rejected, the retry queue's recreation waits for the definitions lock
          sleep 100.milliseconds
          # Not through the definitions, like an auto-delete when the last consumer cancels
          spawn { prim.delete; deleted.close }
          sleep 100.milliseconds
        end
        select
        when deleted.receive?
        when timeout(5.seconds)
          fail "the queue delete deadlocked with the retry queue's recreation"
        end
        vhost.declare_queue("after", true, false)
        vhost.queue?("after").should_not be_nil
      end
    end
  end

  # The channel holds its unack lock while it rejects the messages of a
  # multiple nack: the first reject frees prefetch capacity, so the consumer's
  # deliver loop waits for that lock, and a queue delete through the
  # definitions waits for the deliver loop while it holds the definitions lock
  it "deletes a queue while a nack multiple recreates its retry queue, and the recreation waits for a drain" do
    with_amqp_server do |s|
      vhost = s.vhosts["/"]
      with_channel(s) do |ch|
        ch.prefetch(5)
        q = ch.queue("prim", args: AMQP::Client::Arguments.new({"x-delayed-retry-min" => 60_000}))
        20.times { q.publish_confirm "x" }
        vhost.queue("amq.retry-prim").as(LavinMQ::AMQP::Queue).close
        tags = Channel(UInt64).new(100)
        q.subscribe(no_ack: false) { |m| tags.send m.delivery_tag }
        5.times { tags.receive } # prefetch is full
        deleted = Channel(Nil).new
        with_drain_held do
          ch.basic_nack(5u64, requeue: true, multiple: true)
          sleep 50.milliseconds
          spawn { vhost.delete_queue("prim"); deleted.close }
          sleep 50.milliseconds
        end
        select
        when deleted.receive?
        when timeout(5.seconds)
          fail "the queue delete deadlocked with the nack's recreation of the retry queue"
        end
        vhost.declare_queue("after", true, false)
        vhost.queue?("after").should_not be_nil
      end
    end
  end

  it "deletes a queue while a nack multiple recreates its retry queue, and the definitions lock is held" do
    with_amqp_server do |s|
      vhost = s.vhosts["/"]
      with_channel(s) do |ch|
        ch.prefetch(5)
        q = ch.queue("prim", args: AMQP::Client::Arguments.new({"x-delayed-retry-min" => 60_000}))
        20.times { q.publish_confirm "x" }
        vhost.queue("amq.retry-prim").as(LavinMQ::AMQP::Queue).close
        tags = Channel(UInt64).new(100)
        q.subscribe(no_ack: false) { |m| tags.send m.delivery_tag }
        5.times { tags.receive }
        deleted = Channel(Nil).new
        spawn do
          vhost.synchronize_definitions do
            ch.basic_nack(5u64, requeue: true, multiple: true)
            sleep 100.milliseconds
            vhost.delete_queue("prim")
          end
          deleted.close
        end
        select
        when deleted.receive?
        when timeout(5.seconds)
          fail "the queue delete deadlocked with the nack's recreation of the retry queue"
        end
        vhost.queue?("amq.retry-prim").should be_nil
      end
    end
  end

  it "deletes the retry queue in use when the one it replaced is deleted by name before the new one is registered" do
    with_amqp_server do |s|
      vhost = s.vhosts["/"]
      with_channel(s) do |ch|
        q = ch.queue("prim", args: AMQP::Client::Arguments.new({"x-delayed-retry-min" => 60_000}))
        2.times { q.publish_confirm "x" }
        prim = vhost.queue("prim").as(LavinMQ::AMQP::Queue)
        closed = vhost.queue("amq.retry-prim").as(LavinMQ::AMQP::Queue)
        closed.close
        tags = Channel(UInt64).new(10)
        q.subscribe(no_ack: false) { |m| tags.send m.delivery_tag }
        2.times { tags.receive }
        vhost.synchronize_definitions do
          ch.basic_reject(1u64, requeue: true)
          # The recreated retry queue waits for this lock to be registered
          wait_for { prim.@delayed_retry_queue.try { |r| !r.same?(closed) && r.message_count == 1 } }
          vhost.queue("amq.retry-prim").should be closed
          vhost.delete_queue("amq.retry-prim")
        end
        recreated = prim.@delayed_retry_queue.should_not be_nil
        # Deleted with it, not left running with its files gone
        wait_for { recreated.closed? }
        vhost.queue?("amq.retry-prim").should be_nil
      end
    end
  end

  it "delays the rejected messages in the retry queue a nack multiple recreates, and registers it" do
    with_amqp_server do |s|
      vhost = s.vhosts["/"]
      with_channel(s) do |ch|
        ch.prefetch(5)
        q = ch.queue("prim", args: AMQP::Client::Arguments.new({"x-delayed-retry-min" => 60_000}))
        5.times { q.publish_confirm "x" }
        closed = vhost.queue("amq.retry-prim").as(LavinMQ::AMQP::Queue)
        closed.close
        tags = Channel(UInt64).new(100)
        q.subscribe(no_ack: false) { |m| tags.send m.delivery_tag }
        5.times { tags.receive }
        ch.basic_nack(5u64, requeue: true, multiple: true)
        wait_for { (rq = vhost.queue?("amq.retry-prim")) && !rq.same?(closed) }
        recreated = vhost.queue("amq.retry-prim")
        recreated.closed?.should be_false
        wait_for { recreated.message_count == 5 }
        vhost.queue("prim").message_count.should eq 0
      end
    end
  end

  it "restarts a closed retry queue through its primary queue, which releases its delayed messages" do
    with_amqp_server do |s|
      vhost = s.vhosts["/"]
      with_channel(s) do |ch|
        q = ch.queue("prim", args: AMQP::Client::Arguments.new({"x-delayed-retry-min" => 300}))
        reject_once_into_retry_queue(q)
        prim = vhost.queue("prim").as(LavinMQ::AMQP::Queue)
        q.publish "x"
        wait_for { vhost.queue("amq.retry-prim").message_count == 1 }
        retry_queue = vhost.queue("amq.retry-prim").as(LavinMQ::AMQP::Queue)
        retry_queue.close
        # Like PUT /api/queues/:vhost/:name/restart, without another reject
        retry_queue.restart!.should be_true
        recreated = vhost.queue("amq.retry-prim").as(LavinMQ::AMQP::Queue)
        recreated.should_not be retry_queue
        recreated.closed?.should be_false
        wait_for { prim.@ack_count.get == 1 } # released back to prim and acked
      end
    end
  end

  it "waits for a drain, not a syncfs, before it recreates a retry queue that closed itself, also when it has no unsynced copies" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("prim", args: AMQP::Client::Arguments.new({"x-delayed-retry-min" => 60_000}))
        reject_once_into_retry_queue(q)
        q.publish "x"
        wait_for { s.vhosts["/"].queue("amq.retry-prim").message_count == 1 }
        retry_queue = s.vhosts["/"].queue("amq.retry-prim").as(LavinMQ::AMQP::Queue)
        retry_queue.close
        retry_queue.@msg_store.copies_synced?.should be_true
        drains = s.persister.drains
        synced = s.persister.syncs.size
        q.publish "y" # rejected into the retry queue, which is recreated
        wait_for { s.vhosts["/"].queue("amq.retry-prim").message_count == 2 }
        s.persister.drains.should be > drains
        s.persister.syncs[synced..].none?(&.syncfs).should be_true
      end
    end
  end

  it "syncs the copies of a queue's messages before the queue is deleted" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        src = declare_dead_lettering(ch)
        src_dir = queue_dir(s, "src")
        queue = s.vhosts["/"].queue("src").as(LavinMQ::AMQP::Queue)
        deleted = Channel(Nil).new
        with_drain_held do
          8.times { src.publish "x" * 100_000 }
          wait_for { s.vhosts["/"].queue("dst").message_count == 8 }
          spawn { queue.delete; deleted.close }
          sleep 50.milliseconds
          # The delete waits for the sync, the moved-out segment is still there
          File.exists?(File.join(src_dir, "msgs.0000000001")).should be_true
        end
        deleted.receive?
        Dir.exists?(src_dir).should be_false
        synced_in?(s, queue_dir(s, "dst")).should be_true
        s.persister.syncs.none?(&.syncfs).should be_true
      end
    end
  end

  it "doesn't sync before unlinking a segment that was only consumed" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("plain")
        q.subscribe(no_ack: false, &.ack)
        8.times { q.publish "x" * 100_000 }
        segment = File.join(queue_dir(s, "plain"), "msgs.0000000001")
        wait_for { !File.exists?(segment) }
        s.persister.last_sync.should be_nil
      end
    end
  end

  it "doesn't sync before an unlink when sync is disabled" do
    with_amqp_server do |s|
      LavinMQ::Config.instance.sync = false
      with_channel(s) do |ch|
        src = declare_dead_lettering(ch)
        8.times { src.publish "x" * 100_000 }
        src_segment = File.join(queue_dir(s, "src"), "msgs.0000000001")
        wait_for { !File.exists?(src_segment) }
        s.persister.drains.should eq 0
      end
    end
  end
end
