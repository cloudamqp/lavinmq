require "./spec_helper"

class LavinMQ::Persister
  getter tx_spec_syncfs_count = Atomic(Int32).new(0)
  getter tx_spec_acks_synced = Atomic(Int32).new(0)
  getter tx_spec_messages_synced = Atomic(Int32).new(0)

  protected def sync_file(mfile : MFile) : Nil
    previous_def
    @tx_spec_acks_synced.add(1) if File.basename(mfile.path).starts_with?("acks.")
    @tx_spec_messages_synced.add(1) if File.basename(mfile.path).starts_with?("msgs.")
  end

  protected def syncfs : Nil
    previous_def
    @tx_spec_syncfs_count.add(1)
  end
end

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

    # Commits are released by the publish confirm loop's drain (the only
    # place that msyncs); interleave them with confirm publishes to exercise
    # a drain serving acks and commit waiters at once.
    it "can be commited while publish confirms are in flight" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          q = ch.queue("tx_with_confirms", durable: true)
          done = Channel(Nil).new
          spawn(name: "confirm publisher spec") do
            with_channel(s) do |confirm_ch|
              cq = confirm_ch.queue("tx_with_confirms", durable: true)
              20.times { cq.publish_confirm "confirmed" }
            end
          ensure
            done.send nil
          end
          ch.tx_select
          10.times do |i|
            q.publish "tx #{i}"
            ch.tx_commit
          end
          done.receive
          s.vhosts["/"].queue("tx_with_confirms").message_count.should eq 30
        end
      end
    end

    it "can be commited when sync is disabled" do
      LavinMQ::Config.instance.sync = false
      with_amqp_server do |s|
        with_channel(s) do |ch|
          ch.tx_select
          q = ch.queue
          q.publish "no sync"
          ch.tx_commit
          q.get.not_nil!("expected a message").body_io.to_s.should eq "no sync"
        end
      end
    ensure
      LavinMQ::Config.instance.sync = true
    end
  end

  describe "acks" do
    it "marks the dead-letter destination for sync when rejecting transactionally" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          target = ch.queue("tx_dlx_target", durable: true)
          args = AMQP::Client::Arguments.new
          args["x-dead-letter-exchange"] = ""
          args["x-dead-letter-routing-key"] = target.name
          q = ch.queue("tx_dlx_source", durable: true, args: args)
          ch.tx_select
          q.publish "message"
          ch.tx_commit
          q.get(no_ack: false).not_nil!.reject(requeue: false)
          persister = s.vhosts["/"].persister
          before = persister.tx_spec_messages_synced.get
          ch.tx_commit
          persister.tx_spec_messages_synced.get.should be > before
          target.get.not_nil!.body_io.to_s.should eq("message")
        end
      end
    end

    {% for disposition in [:ack, :reject, :priority_ack] %}
    it "msyncs acknowledgment files for transactional {{ disposition.id }} without syncfs" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          ch.tx_select
          args = AMQP::Client::Arguments.new
          {% if disposition == :priority_ack %}
            args["x-max-priority"] = 5
          {% end %}
          q = ch.queue("tx_ack_durability", durable: true, args: args)
          q.publish "message"
          ch.tx_commit
          msg = q.get(no_ack: false).not_nil!
          {% if disposition != :reject %}
            msg.ack
          {% else %}
            msg.reject(requeue: false)
          {% end %}
          persister = s.vhosts["/"].persister
          before = persister.tx_spec_acks_synced.get
          syncfs_before = persister.tx_spec_syncfs_count.get
          ch.tx_commit
          persister.tx_spec_acks_synced.get.should be > before
          persister.tx_spec_syncfs_count.get.should eq(syncfs_before)
          q.get.should be_nil
        end
      end
    end
    {% end %}

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
