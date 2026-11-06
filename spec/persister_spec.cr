require "./spec_helper"

private def queue_dir(s : LavinMQ::Server, queue_name : String) : String
  File.join(s.vhosts["/"].data_dir, Digest::SHA1.hexdigest(queue_name))
end

private def last_sync(s : LavinMQ::Server) : LavinMQ::Persister::SyncRecord
  s.persister.last_sync.should_not be_nil
end

class LavinMQ::Persister
  {% unless flag?(:release) %}
    def spec_forget_last_sync : Nil
      @last_sync = nil
    end
  {% end %}
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

describe LavinMQ::Persister, "messages the broker moves between queues" do
  it "syncs the filesystem before an emptied segment is unlinked" do
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
        last_sync(s).syncfs.should be_true
      end
    end
  end

  it "syncs before unlinking a priority queue segment that messages were moved out of" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        ch.queue("dst")
        src = ch.queue("src", args: AMQP::Client::Arguments.new({
          "x-max-priority"            => 1,
          "x-message-ttl"             => 1,
          "x-dead-letter-exchange"    => "",
          "x-dead-letter-routing-key" => "dst",
        }))
        8.times { src.publish "x" * 100_000 }
        src_segment = File.join(queue_dir(s, "src"), "prio.000", "msgs.0000000001")
        wait_for { !File.exists?(src_segment) }
        last_sync(s).syncfs.should be_true
      end
    end
  end

  it "syncs before unlinking a delayed exchange segment that messages were released from" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        x = ch.exchange("delayed", "direct", args: AMQP::Client::Arguments.new({"x-delayed-exchange" => true}))
        ch.queue("dst").bind(x.name, "dst")
        props = AMQ::Protocol::Properties.new(headers: AMQP::Client::Arguments.new({"x-delay" => 1}))
        8.times { x.publish "x" * 100_000, "dst", props: props }
        delayed_segment = File.join(queue_dir(s, "amq.delayed-delayed"), "msgs.0000000001")
        wait_for { !File.exists?(delayed_segment) }
        last_sync(s).syncfs.should be_true
      end
    end
  end

  it "syncs before unlinking a segment that messages were rejected into the retry queue from" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("prim", args: AMQP::Client::Arguments.new({"x-delayed-retry-min" => 60_000}))
        reject_once_into_retry_queue(q)
        8.times { q.publish "x" * 100_000 }
        wait_for { !File.exists?(File.join(queue_dir(s, "prim"), "msgs.0000000001")) }
        last_sync(s).syncfs.should be_true
      end
    end
  end

  it "syncs before unlinking a retry queue segment that messages were released from" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("prim", args: AMQP::Client::Arguments.new({"x-delayed-retry-min" => 500}))
        reject_once_into_retry_queue(q)
        8.times { q.publish "x" * 100_000 }
        # The primary's own unlink, after the rejects, syncs first
        wait_for { !File.exists?(File.join(queue_dir(s, "prim"), "msgs.0000000001")) }
        s.persister.spec_forget_last_sync
        wait_for { !File.exists?(File.join(queue_dir(s, "amq.retry-prim"), "msgs.0000000001")) }
        last_sync(s).syncfs.should be_true
      end
    end
  end

  it "syncs before unlinking a retry queue segment that messages expired out of" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        ch.queue("dst")
        q = ch.queue("prim", args: AMQP::Client::Arguments.new({
          "x-delayed-retry-min"       => 60_000,
          "x-message-ttl"             => 500,
          "x-dead-letter-exchange"    => "",
          "x-dead-letter-routing-key" => "dst",
        }))
        reject_once_into_retry_queue(q)
        8.times { q.publish "x" * 100_000 }
        wait_for { !File.exists?(File.join(queue_dir(s, "prim"), "msgs.0000000001")) }
        s.persister.spec_forget_last_sync
        wait_for { !File.exists?(File.join(queue_dir(s, "amq.retry-prim"), "msgs.0000000001")) }
        last_sync(s).syncfs.should be_true
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
        ch.queue("dst")
        src = ch.queue("src", args: AMQP::Client::Arguments.new({
          "x-dead-letter-exchange"    => "",
          "x-dead-letter-routing-key" => "dst",
        }))
        dead_letter = true
        src.subscribe(no_ack: false) { |m| dead_letter ? m.reject : m.ack }
        LavinMQ::Config.instance.sync = false
        # Five messages fill segment 1, and all of them are dead-lettered
        5.times { src.publish "x" * 100_000 }
        wait_for { s.vhosts["/"].queue("dst").message_count == 5 }
        # From here on only plain acks: the rollover unlinks segment 1 while
        # sync is disabled, so only the earlier copies can call for a sync
        dead_letter = false
        src.publish "x" * 100_000
        wait_for { !File.exists?(File.join(queue_dir(s, "src"), "msgs.0000000001")) }
        s.persister.last_sync.should be_nil
        LavinMQ::Config.instance.sync = true
        5.times { src.publish "x" * 100_000 }
        wait_for { !File.exists?(File.join(queue_dir(s, "src"), "msgs.0000000002")) }
        last_sync(s).syncfs.should be_true
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

  it "syncs before the first unlink after a restart, when a store was loaded with acks" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("restarted")
        8.times { q.publish "x" * 100_000 }
        q.get(no_ack: true).should_not be_nil
      end
      queue = s.vhosts["/"].queue("restarted").as(LavinMQ::AMQP::Queue)
      queue.close
      queue.restart!.should be_true
      s.persister.spec_forget_last_sync
      with_channel(s) do |ch|
        ch.queue("restarted", passive: true).subscribe(no_ack: false, &.ack)
        wait_for { !File.exists?(File.join(queue_dir(s, "restarted"), "msgs.0000000001")) }
        last_sync(s).syncfs.should be_true
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
