require "./spec_helper"

private def queue_dir(s : LavinMQ::Server, queue_name : String) : String
  File.join(s.vhosts["/"].data_dir, Digest::SHA1.hexdigest(queue_name))
end

private def last_sync(s : LavinMQ::Server) : LavinMQ::Persister::SyncRecord
  s.persister.last_sync.should_not be_nil
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
end
