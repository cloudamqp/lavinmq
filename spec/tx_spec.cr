require "./spec_helper"

describe "Transactions" do
  describe "publishes" do
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
            msg = q.get(no_ack: false).not_nil!
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
            msg = q.get(no_ack: false).not_nil!
            msg.body_io.to_s.should eq "#{i}"
            msg.ack
          end
          ch.tx_rollback
          ch.basic_recover(requeue: true)
          q.message_count.should eq 2
        end
      end
    end
  end
end
