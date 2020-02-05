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
        when max_acks + 1
          sleep 0.1
          queue.ack_file_size.should eq 0 * sp_size
          queue.enq_file_size.should eq 1 * sp_size
          q.unsubscribe("tag")
        end
      end
    end
  end
end

describe AvalancheMQ::VHost do
  it "GC segments" do
    vhost = s.vhosts["/"]

    segments = ->{ Dir.new(vhost.data_dir).children.select!(/^msgs\./) }

    msg_size = 1024
    msgs_to_fill_a_segment = AvalancheMQ::Config.instance.segment_size // msg_size
    with_channel do |ch|
      ch.confirm_select
      msgid = 0_u64
      q = ch.queue("d", durable: true)
      msgs_to_fill_a_segment.times do
        msgid = q.publish "a" * msg_size
      end
      ch.wait_for_confirm(msgid)

      sleep 0.1
      segments.call.size.should eq 2

      msgs_to_fill_a_segment.times do
        q.get(no_ack: true)
      end
      sleep 1.5
      segments.call.size.should eq 1
    end
  end
end
