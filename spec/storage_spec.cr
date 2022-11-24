require "./spec_helper"

describe LavinMQ::DurableQueue do
  context "after migration between index version 2 to 3" do
    before_each do
      FileUtils.cp_r("./spec/resources/data_dir_index_v2", "/tmp/lavinmq-spec-index-v2")
    end

    after_each do
      FileUtils.rm_rf("/tmp/lavinmq-spec-index-v2")
    end

    it "should succefully convert queue index" do
      s = LavinMQ::Server.new("/tmp/lavinmq-spec-index-v2")
      begin
        q = s.vhosts["/"].queues["queue"].as(LavinMQ::DurableQueue)
        q.basic_get(true) do |env|
          String.new(env.message.body).to_s.should eq "message"
        end.should be_true
      ensure
        s.close
      end
    end
  end

  context "with corrupt segments" do
    context "when consumed" do
      it "should be closed" do
        with_vhost("corrupt_vhost") do |vhost|
          with_channel(vhost: vhost.name) do |ch|
            q = ch.queue("corrupt_q")
            queue = vhost.queues["corrupt_q"].as(LavinMQ::DurableQueue)
            q.publish_confirm "test message"

            bytes = "aaaaauaoeuaoeu".to_slice
            vhost.@segments.each do |_i, mfile|
              mfile.seek(-bytes.size, IO::Seek::Current)
              mfile.write(bytes)
            end

            q.subscribe(tag: "tag", no_ack: false, &.ack)

            should_eventually(be_true) { queue.state.closed? }
          end
        end
      end
    end

    it "should ignore corrupt endings" do
      with_vhost("corrupt_vhost") do |vhost|
        enq_path = ""
        with_channel(vhost: vhost.name) do |ch|
          q = ch.queue("corrupt_q2")
          queue = vhost.queues["corrupt_q2"].as(LavinMQ::DurableQueue)
          enq_path = queue.@enq.path
          2.times do |i|
            q.publish_confirm "test message #{i}"
          end
        end
        close_servers
        File.open(enq_path, "r+") { |f| f.truncate(f.size - 3) }
        TestHelpers.setup
        with_channel(vhost: vhost.name) do |ch|
          q = ch.queue_declare("corrupt_q2", passive: true)
          q[:message_count].should eq 1
        end
      end
    end
  end

  it "GC message index after MAX_ACKS" do
    sp_size = LavinMQ::SegmentPosition::BYTESIZE
    max_acks = LavinMQ::Config.instance.queue_max_acks
    with_channel do |ch|
      ch.prefetch(1)
      q = ch.queue("d", durable: true)
      queue = s.vhosts["/"].queues["d"].as(LavinMQ::DurableQueue)
      queue.enq_file_size.should eq sizeof(Int32)
      max_acks.times do
        q.publish_confirm "", props: AMQP::Client::Properties.new(delivery_mode: 2_u8)
      end
      2.times do
        q.publish_confirm "", props: AMQP::Client::Properties.new(delivery_mode: 2_u8)
      end
      queue.enq_file_size.should eq((max_acks + 2) * sp_size + sizeof(Int32))
      acks = 0
      q.subscribe(tag: "tag", no_ack: false, block: true) do |msg|
        msg.ack
        acks += 1
        case acks
        when max_acks - 1
          sleep 0.1
          queue.ack_file_size.should eq (max_acks - 1) * sp_size + sizeof(Int32)
        when max_acks
          sleep 0.2
          queue.enq_file_size.should eq 2 * sp_size + sizeof(Int32)
          queue.ack_file_size.should eq 0 * sp_size + sizeof(Int32)
          q.unsubscribe("tag")
        end
      end
    ensure
      ch.queue_delete "d"
    end
  end

  pending "GC message index when msgs are dead-lettered" do
    sp_size = LavinMQ::SegmentPosition::BYTESIZE
    max_acks = LavinMQ::Config.instance.queue_max_acks
    with_channel do |ch|
      args = AMQP::Client::Arguments.new
      args["x-max-length"] = 1_i64
      q = ch.queue("ml", durable: true, args: args)
      queue = s.vhosts["/"].queues["ml"].as(LavinMQ::DurableQueue)
      queue.enq_file_size.should eq sizeof(Int32)
      max_acks.times do
        q.publish_confirm "", props: AMQP::Client::Properties.new(delivery_mode: 2_u8)
      end
      1.times do
        q.publish_confirm "", props: AMQP::Client::Properties.new(delivery_mode: 2_u8)
      end
      queue.enq_file_size.should eq(1 * sp_size + sizeof(Int32))
      q.subscribe(tag: "tag", no_ack: false, block: true) do |msg|
        msg.ack
        sleep 0.2
        queue.ack_file_size.should eq 1 * sp_size + sizeof(Int32)
        queue.enq_file_size.should eq 1 * sp_size + sizeof(Int32)
        q.unsubscribe("tag")
      end
    end
  end

  it "it truncates preallocated index files on boot" do
    msg_count = 2_000
    enq_path = ""
    with_channel do |ch|
      q = ch.queue("pre", durable: true)
      msg_count.times { q.publish "" }
      queue = s.vhosts["/"].queues["pre"].as(LavinMQ::DurableQueue)
      enq_path = queue.@enq.path
    end
    close_servers
    # emulate the file was preallocated after server crash
    File.open(enq_path, "r+") { |f| f.truncate(f.size + 24 * 1024**2) }
    TestHelpers.setup
    queue = s.vhosts["/"].queues["pre"].as(LavinMQ::DurableQueue)
    # make sure that the @ready capacity doesn't take into account the preallocated size
    queue.@ready.capacity.should eq Math.pw2ceil(msg_count)
    queue.@ready.size.should eq msg_count
  end

  # Index corruption bug
  # https://github.com/cloudamqp/lavinmq/pull/384
  it "must find messages written after a uncompacted hole" do
    enq_path = ""
    with_channel do |ch|
      q = ch.queue("corruption_test", durable: true)
      q.publish_confirm "Hello world"
      queue = s.vhosts["/"].queues["corruption_test"].as(LavinMQ::DurableQueue)
      enq_path = queue.@enq.path
    end
    close_servers
    # Emulate the file was preallocated after server crash
    File.open(enq_path, "r+") { |f| f.truncate(f.size + 24 * 1024**2) }
    TestHelpers.setup
    # Write another message after the prealloced space
    with_channel do |ch|
      q = ch.queue("corruption_test", durable: true)
      q.publish_confirm "Hello world"
    end
    close_servers
    TestHelpers.setup
    queue = s.vhosts["/"].queues["corruption_test"].as(LavinMQ::DurableQueue)
    queue.@ready.size.should eq 2
  end
end

describe LavinMQ::VHost do
  it "should not be dirty if nothing happend" do
    s.vhosts.create("not_dirty").dirty?.should be_false
  end

  it "should be dirty after ack" do
    with_channel do |ch|
      q = ch.queue
      q.publish "msg"
      q.subscribe(no_ack: true) do |_msg|
      end
      should_eventually(be_true) { s.vhosts["/"].dirty? }
    end
  end

  it "should be dirty after reject" do
    with_channel do |ch|
      q = ch.queue
      q.publish "msg"
      q.subscribe(no_ack: false) do |msg|
        msg.reject
      end
      should_eventually(be_true) { s.vhosts["/"].dirty? }
    end
  end

  it "should be dirty after expire" do
    with_channel do |ch|
      q = ch.queue
      q.publish_confirm "expired", props: AMQP::Client::Properties.new(expiration: "0")
      s.vhosts["/"].dirty?.should be_true
    end
  end

  pending "GC segments" do
    vhost = s.vhosts["/"]
    vhost.queues.each_value &.delete
    vhost.queues.clear

    msg_size = 5120
    overhead = 21
    body = Bytes.new(msg_size)

    segments = ->{ Dir.new(vhost.data_dir).children.select!(/^msgs\./) }
    sleep LavinMQ::Config.instance.gc_segments_interval + 0.2

    size_of_current_segment = File.size(File.join(vhost.data_dir, segments.call.last))

    msgs_to_fill_2_segments = ((LavinMQ::Config.instance.segment_size * 2 - size_of_current_segment) / (msg_size + overhead)).ceil.to_i

    with_channel do |ch|
      ch.confirm_select
      msgid = 0_u64
      q = ch.queue("dd", durable: true)
      msgs_to_fill_2_segments.times do
        msgid = q.publish body
      end
      ch.wait_for_confirm(msgid)
      segments.call.size.should eq 2
      q.purge
      sleep LavinMQ::Config.instance.gc_segments_interval + 0.4
      segments.call.size.should eq 1
    end
  end
end
