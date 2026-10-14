require "./spec_helper"

describe "Transactions" do
  describe "publishes" do
    {% if flag?(:linux) %}
      it "advises the segments they're written to for random access" do
        with_amqp_server do |s|
          with_channel(s) do |ch|
            ch.tx_select
            q = ch.queue("tx_random_access")
            q.publish "m"
            ch.tx_commit
            queue = s.vhosts["/"].queue("tx_random_access").as(LavinMQ::AMQP::Queue)
            vm_flags(queue.@msg_store.@wfile.path).should contain "rr"
          end
        end
      end
    {% end %}

    it "can be commited" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          ch.tx_select
          q = ch.queue
          2.times do |i|
            q.publish "#{i}" * 200_000
          end
          q.get.should be_nil
          ch.tx_commit
          2.times do |i|
            msg = q.get
            if msg
              msg.body_io.to_s.should eq "#{i}" * 200_000
            else
              msg.should_not be_nil
            end
          end
        end
      end
    end

    it "can be commited to multiple queues" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          ch.tx_select
          q1 = ch.queue
          q2 = ch.queue
          q1.bind("amq.fanout", "")
          q2.bind("amq.fanout", "")
          x = ch.exchange("amq.fanout", "fanout")
          2.times do |i|
            x.publish i.to_s * 200_000, ""
            ch.basic_publish("", "", "")
          end
          q1.get.should be_nil
          ch.tx_commit
          2.times do |i|
            if msg = q1.get
              msg.body_io.to_s.should eq i.to_s * 200_000
            else
              msg.should_not be_nil
            end
            if msg = q2.get
              msg.body_io.to_s.should eq i.to_s * 200_000
            else
              msg.should_not be_nil
            end
          end
        end
      end
    end

    it "can be rollbacked" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          ch.tx_select
          q = ch.queue
          q.publish ""
          q.get.should be_nil
          ch.tx_rollback
          q.get.should be_nil
          q.message_count.should eq 0
        end
      end
    end
  end

  describe "acks" do
    it "can be commited" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          ch.tx_select
          q = ch.queue
          2.times { |i| q.publish "#{i}" }
          ch.tx_commit
          2.times do |i|
            msg = q.get(no_ack: false).should_not be_nil
            msg.body_io.to_s.should eq "#{i}"
            msg.ack
          end
          ch.tx_commit
          ch.basic_recover(requeue: true)
          q.message_count.should eq 0
        end
      end
    end

    it "can be rollbacked" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          ch.tx_select
          q = ch.queue
          2.times { |i| q.publish "#{i}" }
          ch.tx_commit
          2.times do |i|
            msg = q.get(no_ack: false).should_not be_nil
            msg.body_io.to_s.should eq "#{i}"
            msg.ack
          end
          ch.tx_rollback
          ch.basic_recover(requeue: true)
          q.message_count.should eq 2
        end
      end
    end

    it "treats ack of all with nothing outstanding as a no-op" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          ch.tx_select
          ch.basic_ack(0_u64, multiple: true)
          ch.tx_commit
          q = ch.queue # the connection must still be usable
          q.publish "m1"
          ch.tx_commit
          q.get(no_ack: true).not_nil!.body_io.to_s.should eq "m1"
        end
      end
    end

    it "keeps a tx-pending ack across basic.recover" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          q = ch.queue("txrec")
          q.publish "m1"
          msgs = Channel(AMQP::Client::DeliverMessage).new
          q.subscribe(no_ack: false) { |m| msgs.send m }
          msg = msgs.receive
          ch.tx_select
          ch.basic_ack(msg.delivery_tag)
          ch.basic_recover(requeue: true) # must not drop the tx-pending tag
          ch.tx_commit                    # applies tx acks before sending CommitOk
          sq = s.vhosts["/"].queues.find! &.name.== "txrec"
          sq.unacked_count.should eq 0
          sq.message_count.should eq 0
        end
      end
    end
  end
end
