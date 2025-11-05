require "./spec_helper"

describe LavinMQ::AMQP::DurableQueue do
  context "after migration between index version 2 to 3" do
    before_each do
      FileUtils.cp_r("./spec/resources/data_dir_index_v2", "/tmp/lavinmq-spec-index-v2")
    end

    after_each do
      FileUtils.rm_rf("/tmp/lavinmq-spec-index-v2")
    end

    it "should successfully convert queue index" do
      config = LavinMQ::Config.new.tap &.data_dir = "/tmp/lavinmq-spec-index-v2"
      server = LavinMQ::Server.new(config)
      begin
        q = server.vhosts["/"].queues["queue"].as(LavinMQ::AMQP::DurableQueue)
        q.basic_get(true) do |env|
          String.new(env.message.body).to_s.should eq "message"
        end.should be_true
      ensure
        server.close
      end
    end
  end

  context "with corrupt segments" do
    context "when consumed" do
      it "should be closed" do
        with_amqp_server do |s|
          vhost = s.vhosts.create("corrupt_vhost")
          with_channel(s, vhost: vhost.name) do |ch|
            q = ch.queue("corrupt_q")
            queue = vhost.queues["corrupt_q"].as(LavinMQ::AMQP::DurableQueue)
            q.publish_confirm "test message"

            sleep 10.milliseconds
            bytes = "111111111aaaaauaoeuaoeu".to_slice
            queue.@msg_store.@segments.each_value do |mfile|
              File.open(mfile.path, "w+") do |f|
                f.seek(mfile.size - bytes.size)
                f.write(bytes)
              end
            end

            q.subscribe(tag: "tag", no_ack: false, &.ack)

            should_eventually(be_true) { queue.state.closed? }
          end

          vhost.queues["corrupt_q"].try &.delete
        end
      end
    end

    it "should ignore corrupt endings" do
      with_amqp_server do |s|
        vhost = s.vhosts.create("corrupt_vhost")
        enq_path = ""
        with_channel(s, vhost: vhost.name) do |ch|
          q = ch.queue("corrupt_q2")
          queue = vhost.queues["corrupt_q2"].as(LavinMQ::AMQP::DurableQueue)
          enq_path = queue.@msg_store.@segments.last_value.path
          2.times do |i|
            q.publish_confirm "test message #{i}"
          end
        end
        s.stop
        File.open(enq_path, "r+") { |f| f.truncate(f.size - 3) }
        s.restart
        with_channel(s, vhost: vhost.name) do |ch|
          q = ch.queue_declare("corrupt_q2", passive: true)
          q[:message_count].should eq 1
        end
      end
    end
  end

  # Index corruption bug
  # https://github.com/cloudamqp/lavinmq/pull/384
  it "must find messages written after a uncompacted hole" do
    with_amqp_server do |s|
      enq_path = ""
      with_channel(s) do |ch|
        q = ch.queue("corruption_test", durable: true)
        q.publish_confirm "Hello world"
        queue = s.vhosts["/"].queues["corruption_test"].as(LavinMQ::AMQP::DurableQueue)
        enq_path = queue.@msg_store.@segments.last_value.path
      end
      s.stop
      # Emulate the file was preallocated after server crash
      File.open(enq_path, "r+") { |f| f.truncate(f.size + 24 * 1024**2) }
      s.restart
      # Write another message after the prealloced space
      with_channel(s) do |ch|
        q = ch.queue("corruption_test", durable: true)
        q.publish_confirm "Hello world"
      end
      s.restart
      queue = s.vhosts["/"].queues["corruption_test"].as(LavinMQ::AMQP::DurableQueue)
      queue.message_count.should eq 2
    end
  end

  it "shift? handles files with few extra bytes" do
    queue_name = Random::Secure.hex(10)
    with_amqp_server do |s|
      vhost = s.vhosts.create("test_vhost")
      with_channel(s, vhost: vhost.name) do |ch|
        q = ch.queue(queue_name)
        queue = vhost.queues[queue_name].as(LavinMQ::AMQP::DurableQueue)
        mfile = queue.@msg_store.@segments.first_value

        # fill up one segment
        message_size = 41
        while mfile.size < (LavinMQ::Config.instance.segment_size - message_size*2)
          q.publish_confirm "a"
        end
        remaining_size = LavinMQ::Config.instance.segment_size - mfile.size - message_size
        q.publish_confirm "a" * remaining_size

        # publish one more message to create a new segment
        q.publish_confirm "a"

        # resize first segment to LavinMQ::Config.instance.segment_size
        File.open(mfile.path, "r+") do |f|
          f.truncate(LavinMQ::Config.instance.segment_size)
        end

        # read messages, should not raise any error
        q.subscribe(tag: "tag", no_ack: false, &.ack)
        should_eventually(be_true) { queue.empty? }
      end
    end
  end

  it "first? handles files with few extra bytes" do
    queue_name = Random::Secure.hex(10)
    with_amqp_server do |s|
      vhost = s.vhosts.create("test_vhost")
      with_channel(s, vhost: vhost.name) do |ch|
        q = ch.queue(queue_name)
        queue = vhost.queues[queue_name].as(LavinMQ::AMQP::DurableQueue)
        mfile = queue.@msg_store.@segments.first_value

        # fill up one segment
        message_size = 41
        while mfile.size < (LavinMQ::Config.instance.segment_size - message_size*2)
          q.publish_confirm "a"
        end
        remaining_size = LavinMQ::Config.instance.segment_size - mfile.size - message_size
        q.publish_confirm "a" * remaining_size

        # publish one more message to create a new segment
        q.publish_confirm "a"

        # resize first segment to LavinMQ::Config.instance.segment_size
        File.open(mfile.path, "r+") do |f|
          f.truncate(LavinMQ::Config.instance.segment_size)
        end

        store = LavinMQ::MessageStore.new(queue.@msg_store.@msg_dir, nil)
        mfile = store.@segments.first_value
        mfile.pos = mfile.size - 2
        if msg = store.first?
          msg.@segment_position.@segment.should eq 2
        else
          fail "no message"
        end
      end
    end
  end

  # ArithmeticOverflow error when routing key length = 255
  # https://github.com/cloudamqp/lavinmq/issues/1093
  it "should handle routing key length = 255" do
    rk = "a" * 255
    with_amqp_server do |s|
      vhost = s.vhosts.create("test_vhost")
      with_channel(s, vhost: vhost.name) do |ch|
        q = ch.queue(rk, durable: true)
        queue = vhost.queues[rk].as(LavinMQ::AMQP::DurableQueue)
        q.publish_confirm "a"
        store = LavinMQ::MessageStore.new(queue.@msg_store.@msg_dir, nil)

        if env = store.shift?
          if msg = env.message
            msg.routing_key.should eq rk
          else
            fail "no message"
          end
        else
          fail "no message"
        end
      end
    end
  end
end

describe LavinMQ::VHost do
  pending "GC segments" do
    with_amqp_server do |s|
      vhost = s.vhosts["/"]
      vhost.queues.each_value &.delete
      vhost.queues.clear

      msg_size = 5120
      overhead = 21
      body = Bytes.new(msg_size)

      segments = -> { Dir.new(vhost.data_dir).children.select!(/^msgs\./) }

      size_of_current_segment = File.size(File.join(vhost.data_dir, segments.call.last))

      msgs_to_fill_2_segments = ((LavinMQ::Config.instance.segment_size * 2 - size_of_current_segment) / (msg_size + overhead)).ceil.to_i

      with_channel(s) do |ch|
        ch.confirm_select
        msgid = 0_u64
        q = ch.queue("dd", durable: true)
        msgs_to_fill_2_segments.times do
          msgid = q.publish body
        end
        ch.wait_for_confirm(msgid)
        segments.call.size.should eq 2
        q.purge
        segments.call.size.should eq 1
      end
    end
  end
end
