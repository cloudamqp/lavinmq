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
      server = LavinMQ::Server.new("/tmp/lavinmq-spec-index-v2")
      begin
        q = server.vhosts["/"].queues["queue"].as(LavinMQ::DurableQueue)
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
        with_vhost("corrupt_vhost") do |vhost|
          with_channel(vhost: vhost.name) do |ch|
            q = ch.queue("corrupt_q")
            queue = vhost.queues["corrupt_q"].as(LavinMQ::DurableQueue)
            q.publish_confirm "test message"

            sleep 0.01
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
        end
      end
    end

    it "should ignore corrupt endings" do
      with_vhost("corrupt_vhost") do |vhost|
        enq_path = ""
        with_channel(vhost: vhost.name) do |ch|
          q = ch.queue("corrupt_q2")
          queue = vhost.queues["corrupt_q2"].as(LavinMQ::DurableQueue)
          enq_path = queue.@msg_store.@segments.last_value.path
          2.times do |i|
            q.publish_confirm "test message #{i}"
          end
        end
        Server.stop
        File.open(enq_path, "r+") { |f| f.truncate(f.size - 3) }
        Server.restart
        with_channel(vhost: vhost.name) do |ch|
          q = ch.queue_declare("corrupt_q2", passive: true)
          q[:message_count].should eq 1
        end
      end
    end
  end

  # Index corruption bug
  # https://github.com/cloudamqp/lavinmq/pull/384
  it "must find messages written after a uncompacted hole" do
    enq_path = ""
    with_channel do |ch|
      q = ch.queue("corruption_test", durable: true)
      q.publish_confirm "Hello world"
      queue = Server.vhosts["/"].queues["corruption_test"].as(LavinMQ::DurableQueue)
      enq_path = queue.@msg_store.@segments.last_value.path
    end
    Server.stop
    # Emulate the file was preallocated after server crash
    File.open(enq_path, "r+") { |f| f.truncate(f.size + 24 * 1024**2) }
    Server.restart
    # Write another message after the prealloced space
    with_channel do |ch|
      q = ch.queue("corruption_test", durable: true)
      q.publish_confirm "Hello world"
    end
    Server.restart
    queue = Server.vhosts["/"].queues["corruption_test"].as(LavinMQ::DurableQueue)
    queue.message_count.should eq 2
  end

  # Delete unused segments bug
  # https://github.com/cloudamqp/lavinmq/pull/565
  it "should remove unused segments after being consumed" do
    with_channel do |ch|
      q_name = "segment_delete_test"
      q = ch.queue(q_name, durable: true)
      10.times do # Publish and consume large messages that ensures new segments are created
        q.publish_confirm Random::DEFAULT.random_bytes(LavinMQ::Config.instance.segment_size)
        q.subscribe(no_ack: false, &.ack)
      end
      queue = Server.vhosts["/"].queues[q_name].as(LavinMQ::DurableQueue)
      queue.@msg_store.@segments.size.should be <= 2
    end
  end
end

describe LavinMQ::VHost do
  pending "GC segments" do
    vhost = Server.vhosts["/"]
    vhost.queues.each_value &.delete
    vhost.queues.clear

    msg_size = 5120
    overhead = 21
    body = Bytes.new(msg_size)

    segments = ->{ Dir.new(vhost.data_dir).children.select!(/^msgs\./) }

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
      segments.call.size.should eq 1
    end
  end
end
