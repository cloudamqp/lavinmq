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

    it "commits quickly (no batching delay)" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          ch.tx_select
          q = ch.queue
          q.publish "msg"
          start = Time.instant
          ch.tx_commit
          duration = Time.instant - start
          duration.should be < 100.milliseconds
        end
      end
    end

    it "commits from multiple channels concurrently without blocking each other" do
      # Tx::CommitOk is sent asynchronously via the same batching loop as
      # publish confirms (see Persister#enqueue_tx_commit); this exercises
      # concurrent commits from many channels being drained together without
      # cross-talk or deadlock.
      with_amqp_server do |s|
        conn = AMQP::Client.new(port: amqp_port(s)).connect
        n = 20
        channels = Array.new(n) { conn.channel }
        queues = channels.map_with_index { |ch, i| ch.queue("tx_concurrent_#{i}") }
        done = Channel(Nil).new(n)
        channels.each_with_index do |ch, i|
          spawn do
            ch.tx_select
            queues[i].publish "msg"
            ch.tx_commit
            done.send nil
          end
        end
        n.times { done.receive }
        queues.each(&.message_count.should(eq(1)))
      ensure
        conn.try &.close(no_wait: false)
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
