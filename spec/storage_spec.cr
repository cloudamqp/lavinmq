require "./spec_helper"

describe AvalancheMQ::DurableQueue do
  it "GC message index after MAX_ACKS" do
    sp_size = sizeof(AvalancheMQ::SegmentPosition)
    max_acks = AvalancheMQ::Config.instance.queue_max_acks
    with_channel do |ch|
      q = ch.queue("d", durable: true)
      queue = s.vhosts["/"].queues["d"].as(AvalancheMQ::DurableQueue)
      queue.enq_file_size.should eq 0
      max_acks.times do
        q.publish_confirm "", props: AMQP::Client::Properties.new(delivery_mode: 2_u8)
      end
      2.times do
        q.publish_confirm "", props: AMQP::Client::Properties.new(delivery_mode: 2_u8)
      end
      queue.enq_file_size.should eq((max_acks + 2) * sp_size)
      acks = 0
      q.subscribe(tag: "tag", no_ack: false, block: true) do |msg|
        msg.ack
        acks += 1
        case acks
        when max_acks - 1
          sleep 0.1
          queue.ack_file_size.should eq (max_acks - 1) * sp_size
        when max_acks
          sleep 0.1
          queue.ack_file_size.should eq 0 * sp_size
          queue.enq_file_size.should eq 2 * sp_size
          q.unsubscribe("tag")
        end
      end
    end
  end

  it "GC message index when msgs are dead-lettered" do
    sp_size = sizeof(AvalancheMQ::SegmentPosition)
    max_acks = AvalancheMQ::Config.instance.queue_max_acks
    with_channel do |ch|
      args = AMQP::Client::Arguments.new
      args["x-max-length"] = 1_i64
      q = ch.queue("ml", durable: true, args: args)
      queue = s.vhosts["/"].queues["ml"].as(AvalancheMQ::DurableQueue)
      queue.enq_file_size.should eq 0
      max_acks.times do
        q.publish_confirm "", props: AMQP::Client::Properties.new(delivery_mode: 2_u8)
      end
      1.times do
        q.publish_confirm "", props: AMQP::Client::Properties.new(delivery_mode: 2_u8)
      end
      queue.enq_file_size.should eq(1 * sp_size)
      q.subscribe(tag: "tag", no_ack: false, block: true) do |msg|
        msg.ack
        sleep 0.2
        queue.ack_file_size.should eq 1 * sp_size
        queue.enq_file_size.should eq 1 * sp_size
        q.unsubscribe("tag")
      end
    end
  end
end

describe AvalancheMQ::VHost do
  it "GC segments" do
    vhost = s.vhosts["/"]
    msg_size = 512
    overhead = 10
    body = Bytes.new(msg_size)

    segments = ->{ Dir.new(vhost.data_dir).children.select!(/^msgs\./) }
    sleep AvalancheMQ::Config.instance.gc_segments_interval + 0.1

    segments_at_start = segments.call.size
    msgs_to_fill_2_segments = (AvalancheMQ::Config.instance.segment_size * 2 / (msg_size + overhead)).ceil.to_i

    with_channel do |ch|
      ch.confirm_select
      msgid = 0_u64
      q = ch.queue("d", durable: true)
      msgs_to_fill_2_segments.times do
        msgid = q.publish body
      end
      ch.wait_for_confirm(msgid)

      sleep 0.1
      segments.call.size.should eq segments_at_start + 2

      q.purge
      sleep AvalancheMQ::Config.instance.gc_segments_interval + 0.2
      segments.call.size.should eq segments_at_start + 1
    end
  end
end
