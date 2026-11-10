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

private def declare_dead_lettering(ch, ttl = 1)
  ch.queue("dst")
  ch.queue("src", args: AMQP::Client::Arguments.new({
    "x-message-ttl"             => ttl,
    "x-dead-letter-exchange"    => "",
    "x-dead-letter-routing-key" => "dst",
  }))
end

describe LavinMQ::Persister, "segments the stores have emptied" do
  it "syncs the whole filesystem before the first unlink, the copies of an earlier process aren't marked" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        src = declare_dead_lettering(ch)
        # Plain publishes: no confirm requests a sync, only the segment unlink can
        8.times { src.publish "x" * 100_000 }
        src_segment = File.join(queue_dir(s, "src"), "msgs.0000000001")
        wait_for { !File.exists?(src_segment) }
        last_sync(s).syncfs.should be_true
      end
    end
  end

  it "syncs the segments the messages were moved to, not the filesystem, before the unlink" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        src = declare_dead_lettering(ch)
        s.persister.sync # a syncfs, nothing is unmarked after it
        8.times { src.publish "x" * 100_000 }
        src_segment = File.join(queue_dir(s, "src"), "msgs.0000000001")
        wait_for { !File.exists?(src_segment) }
        File.exists?("#{src_segment.sub("msgs.", "acks.")}").should be_false
        sync = last_sync(s)
        sync.syncfs.should be_false
        sync.paths.should contain File.join(queue_dir(s, "dst"), "msgs.0000000001")
      end
    end
  end

  it "syncs nothing before the unlink of a segment that consumers emptied" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("acked")
        s.persister.sync
        8.times { q.publish "x" * 100_000 }
        segment = File.join(queue_dir(s, "acked"), "msgs.0000000001")
        q.subscribe(no_ack: false, &.ack)
        wait_for { !File.exists?(segment) }
        sync = last_sync(s)
        sync.syncfs.should be_false
        sync.paths.should be_empty
      end
    end
  end

  it "syncs the whole filesystem before an unlink after sync was disabled, its drains drop the marks" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        src = declare_dead_lettering(ch, ttl: 60_000)
        s.persister.sync
        LavinMQ::Config.instance.sync = false
        ch.confirm_select
        src.publish_confirm "x" # a drain that skips its sync
        LavinMQ::Config.instance.sync = true
        q = ch.queue("acked")
        8.times { q.publish "x" * 100_000 }
        segment = File.join(queue_dir(s, "acked"), "msgs.0000000001")
        q.subscribe(no_ack: false, &.ack)
        wait_for { !File.exists?(segment) }
        last_sync(s).syncfs.should be_true
      end
    ensure
      LavinMQ::Config.instance.sync = true
    end
  end

  it "has unlinked the handed over segments when #sync_marked returns" do
    with_datadir do |data_dir|
      persister = LavinMQ::Persister.new(data_dir)
      Dir.mkdir(File.join(data_dir, "q"))
      store = LavinMQ::MessageStore.new(File.join(data_dir, "q"), nil, persister: persister)
      begin
        msg_size = LavinMQ::Config.instance.segment_size.to_u64 // 2 + 1
        msg = LavinMQ::Message.new(RoughTime.unix_ms, "e", "k",
          AMQ::Protocol::Properties.new, msg_size, IO::Memory.new("a" * msg_size))
        3.times { store.push(msg) }
        env = store.shift?.should_not be_nil
        store.delete(env.segment_position)
        store.@segments.has_key?(1u32).should be_false
        persister.sync_marked
        File.exists?(File.join(data_dir, "q", "msgs.0000000001")).should be_false
        File.exists?(File.join(data_dir, "q", "acks.0000000001")).should be_false
      ensure
        store.close
        persister.close
      end
    end
  end

  it "loads a store only after the segments another instance emptied in its directory are unlinked" do
    with_datadir do |data_dir|
      persister = LavinMQ::Persister.new(data_dir)
      msg_dir = File.join(data_dir, "q")
      Dir.mkdir(msg_dir)
      store = LavinMQ::MessageStore.new(msg_dir, nil, persister: persister)
      begin
        msg_size = LavinMQ::Config.instance.segment_size.to_u64 // 2 + 1
        msg = LavinMQ::Message.new(RoughTime.unix_ms, "e", "k",
          AMQ::Protocol::Properties.new, msg_size, IO::Memory.new("a" * msg_size))
        3.times { store.push(msg) }
        env = store.shift?.should_not be_nil
        store.delete(env.segment_position)
        store.close
        store = LavinMQ::MessageStore.new(msg_dir, nil, persister: persister)
        store.@segments.keys.should eq [2u32, 3u32]
        File.exists?(File.join(msg_dir, "msgs.0000000001")).should be_false
        store.size.should eq 2
      ensure
        store.close
        persister.close
      end
    end
  end

  it "unlinks the segments handed over at shutdown, after the final drain" do
    with_datadir do |data_dir|
      persister = LavinMQ::Persister.new(data_dir)
      Dir.mkdir(File.join(data_dir, "q"))
      store = LavinMQ::MessageStore.new(File.join(data_dir, "q"), nil, persister: persister)
      begin
        msg_size = LavinMQ::Config.instance.segment_size.to_u64 // 2 + 1
        msg = LavinMQ::Message.new(RoughTime.unix_ms, "e", "k",
          AMQ::Protocol::Properties.new, msg_size, IO::Memory.new("a" * msg_size))
        3.times { store.push(msg) }
        env = store.shift?.should_not be_nil
        persister.close
        sleep 50.milliseconds # the loop has exited
        store.delete(env.segment_position)
        File.exists?(File.join(data_dir, "q", "msgs.0000000001")).should be_false
      ensure
        store.close
      end
    end
  end
end
