require "./spec_helper"
require "log/spec"

class LavinMQ::AMQP::Queue
  def spec_retry_delay_for(delivery_count : Int32) : Int64
    retry_delay_for(delivery_count)
  end

  def spec_set_first_delivery_count(count : Int32) : Nil
    @msg_store_lock.synchronize do
      env = @msg_store.first? || raise "queue is empty"
      @deliveries[env.segment_position] = count
    end
  end

  # What #close does first, before it closes the store
  def spec_mark_closing : Nil
    @closed = true
  end

  def spec_close_store : Nil
    @msg_store_lock.synchronize { @msg_store.close }
  end

  # As a release does once it has published the message's copy
  def spec_shift_and_delete_moved : Nil
    env = @msg_store_lock.synchronize { @msg_store.shift? } || raise "queue is empty"
    delete_message(env.segment_position)
  end
end

# Parks dead-lettering inside #publish_internal until released. In the AMQP
# namespace because AMQP::Exchange#bind refuses destinations outside it.
class LavinMQ::AMQP::BlockingDeadLetterQueue < LavinMQ::AMQP::Queue
  getter entered = ::Channel(Nil).new(1)
  getter release = ::Channel(Nil).new

  protected def publish_internal(msg : LavinMQ::Message, dlx_tasks : Argument::DeadLettering::Tasks? = nil) : PublishResult
    @entered.send(nil)
    @release.receive
    super
  end
end

class LavinMQ::AMQP::DelayedQueue::DelayedMessageStore
  class_property? fail_next_push = false
  class_property? fail_next_purge = false

  def purge_all
    if self.class.fail_next_purge?
      self.class.fail_next_purge = false
      raise LavinMQ::MessageStore::Error.new(@wfile)
    end
    previous_def
  end

  def push(msg) : SegmentPosition
    if self.class.fail_next_push?
      self.class.fail_next_push = false
      raise IO::Error.new("No space left on device")
    end
    previous_def
  end
end

class LavinMQ::AMQP::DelayedRetryQueue
  class_property? fail_next_expire = false

  def expire_messages
    if LavinMQ::AMQP::DelayedRetryQueue.fail_next_expire?
      LavinMQ::AMQP::DelayedRetryQueue.fail_next_expire = false
      raise LavinMQ::MessageStore::Error.new(@msg_store.@wfile)
    end
    super
  end
end

class LavinMQ::AMQP::RejectFailingQueue < LavinMQ::AMQP::Queue
  def reject(sp : SegmentPosition, requeue : Bool)
    raise IO::Error.new("Injected reject failure")
  end
end

describe "Retry Queue" do
  describe "Scaffold" do
    it "should create internal retry queue when x-delayed-retry-min is set" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-delivery-limit"    => 3,
            "x-delayed-retry-min" => 1000,
          })
          ch.queue("retry-test", args: args)
          retry_q = s.vhosts["/"].queue?("amq.retry-retry-test").should_not be_nil
          retry_q.internal?.should be_true
        end
      end
    end

    it "should not create retry queue when x-delayed-retry-min is not set" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({"x-delivery-limit" => 3})
          ch.queue("no-retry-test", args: args)
          s.vhosts["/"].queue?("amq.retry-no-retry-test").should be_nil
        end
      end
    end

    it "should reject an invalid x-delayed-retry-min" do
      with_amqp_server do |s|
        {-1, "bad"}.each do |value|
          with_channel(s) do |ch|
            expect_raises(AMQP::Client::Channel::ClosedException) do
              args = AMQP::Client::Arguments.new({
                "x-delivery-limit"    => 3,
                "x-delayed-retry-min" => value,
              })
              ch.queue("bad-retry", args: args)
            end
          end
        end
      end
    end

    it "should keep retrying without x-delivery-limit" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({"x-delayed-retry-min" => 1})
          q = ch.queue("retry-no-limit", args: args)
          ch.default_exchange.publish_confirm("no limit", q.name)

          25.times do
            msg = wait_for(timeout: 5.seconds) { q.get(no_ack: false) }
            msg.reject(requeue: true)
          end

          msg = wait_for(timeout: 5.seconds) { q.get(no_ack: false) }
          msg.properties.headers.try(&.["x-delivery-count"]?).should eq 25
        end
      end
    end

    it "should give a constant delay without looping over a large delivery count" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-delayed-retry-min"        => 200,
            "x-delayed-retry-multiplier" => 1,
          })
          ch.queue("retry-constant-large-count", args: args)
          q = s.vhosts["/"].queue("retry-constant-large-count").as(LavinMQ::AMQP::Queue)
          elapsed = Time.measure do
            q.spec_retry_delay_for(Int32::MAX).should eq 200
          end
          elapsed.should be < 100.milliseconds
        end
      end
    end

    it "should not overflow the delivery count without x-delivery-limit" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({"x-delayed-retry-min" => 1})
          q = ch.queue("retry-count-overflow", args: args)
          ch.default_exchange.publish_confirm("many retries", q.name)
          queue = s.vhosts["/"].queue("retry-count-overflow").as(LavinMQ::AMQP::Queue)
          queue.spec_set_first_delivery_count(Int32::MAX)

          msg = q.get(no_ack: false).should_not be_nil
          msg.properties.headers.try(&.["x-delivery-count"]?).should eq Int32::MAX
        end
      end
    end

    it "should ignore a client-supplied x-delivery-count when retry is not enabled" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({"x-delivery-limit" => 2})
          q = ch.queue("no-retry-forged-count", args: args)
          headers = AMQP::Client::Arguments.new({"x-delivery-count" => 1000})
          q.publish_confirm "forged", props: AMQP::Client::Properties.new(headers: headers)

          msg = wait_for { q.get(no_ack: false) }
          msg.reject(requeue: true)

          msg2 = wait_for { q.get(no_ack: true) }
          msg2.body_io.to_s.should eq "forged"
        end
      end
    end

    it "should count time spent in the retry queue against x-message-ttl" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          dlq = ch.queue("retry-ttl-dlq")
          args = AMQP::Client::Arguments.new({
            "x-delivery-limit"          => 10,
            "x-delayed-retry-min"       => 500,
            "x-message-ttl"             => 300,
            "x-dead-letter-exchange"    => "",
            "x-dead-letter-routing-key" => "retry-ttl-dlq",
          })
          q = ch.queue("retry-ttl", args: args)
          q.publish_confirm "expire me"

          msg = wait_for { q.get(no_ack: false) }
          msg.reject(requeue: true)

          dlq_msg = wait_for(timeout: 5.seconds) { dlq.get(no_ack: true) }
          dlq_msg.body_io.to_s.should eq "expire me"
        end
      end
    end

    it "should dead-letter a delayed message when its TTL runs out before the backoff ends" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          dlq = ch.queue("retry-ttl-early-dlq")
          args = AMQP::Client::Arguments.new({
            "x-delivery-limit"          => 10,
            "x-delayed-retry-min"       => 10_000,
            "x-message-ttl"             => 300,
            "x-dead-letter-exchange"    => "",
            "x-dead-letter-routing-key" => "retry-ttl-early-dlq",
          })
          q = ch.queue("retry-ttl-early", args: args)
          q.publish_confirm "expire me"

          msg = wait_for { q.get(no_ack: false) }
          msg.reject(requeue: true)

          start = Time.instant
          dlq_msg = wait_for(timeout: 5.seconds) { dlq.get(no_ack: true) }
          (Time.instant - start).should be < 2.seconds
          dlq_msg.body_io.to_s.should eq "expire me"
          headers = dlq_msg.properties.headers.should_not be_nil
          headers["x-first-death-reason"].should eq "expired"
          headers["x-first-death-queue"].should eq "retry-ttl-early"
          headers.has_key?("x-delay").should be_false
          s.vhosts["/"].queue("amq.retry-retry-ttl-early").message_count.should eq 0
        end
      end
    end

    it "should accept rejects while a delayed message is being dead-lettered" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        vhost.declare_exchange("retry-dlx", "direct", false, false)
        blocking = LavinMQ::AMQP::BlockingDeadLetterQueue.create(vhost, "retry-dlx-blocking")
        vhost.exchange("retry-dlx").bind(blocking, "blocked", nil)
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-delivery-limit"          => 10,
            "x-delayed-retry-min"       => 10_000,
            "x-dead-letter-exchange"    => "retry-dlx",
            "x-dead-letter-routing-key" => "blocked",
          })
          q = ch.queue("retry-dlx-concurrent", args: args)
          q.publish_confirm "expire me", props: AMQP::Client::Properties.new(expiration: "300")
          q.publish_confirm "reject me"
          retry_q = vhost.queue("amq.retry-retry-dlx-concurrent")

          wait_for { q.get(no_ack: false) }.reject(requeue: true)
          msg = wait_for { q.get(no_ack: false) }
          select
          when blocking.entered.receive
          when timeout(5.seconds)
            fail "message was never dead-lettered"
          end

          begin
            msg.reject(requeue: true)
            wait_for { retry_q.message_count == 1 }
          ensure
            select
            when blocking.release.send(nil)
            when timeout(1.second)
            end
          end
          wait_for { blocking.message_count == 1 }
        end
      end
    end

    it "should dead-letter an expired delayed message instead of delaying it again when the primary queue is full" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          dlq = ch.queue("retry-overflow-expired-dlq")
          args = AMQP::Client::Arguments.new({
            "x-delivery-limit"          => 10,
            "x-delayed-retry-min"       => 100,
            "x-max-length"              => 1,
            "x-overflow"                => "reject-publish",
            "x-dead-letter-exchange"    => "",
            "x-dead-letter-routing-key" => "retry-overflow-expired-dlq",
          })
          q = ch.queue("retry-overflow-expired", args: args)
          q.publish_confirm "retry me", props: AMQP::Client::Properties.new(expiration: "400")

          msg = wait_for { q.get(no_ack: false) }
          q.publish_confirm "block"
          msg.reject(requeue: true)

          dlq_msg = wait_for(timeout: 5.seconds) { dlq.get(no_ack: true) }
          dlq_msg.body_io.to_s.should eq "retry me"
          headers = dlq_msg.properties.headers.should_not be_nil
          headers["x-first-death-reason"].should eq "expired"
          s.vhosts["/"].queue("amq.retry-retry-overflow-expired").message_count.should eq 0
          wait_for { q.get(no_ack: true) }.body_io.to_s.should eq "block"
        end
      end
    end

    it "should delay a retried message again when the primary queue is full" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-delivery-limit"    => 3,
            "x-delayed-retry-min" => 100,
            "x-max-length"        => 1,
            "x-overflow"          => "reject-publish",
          })
          q = ch.queue("retry-overflow-redelay", args: args)
          q.publish_confirm "retry me"

          msg = wait_for { q.get(no_ack: false) }
          q.publish_confirm "block"
          msg.reject(requeue: true)

          sleep 350.milliseconds
          s.vhosts["/"].queue("amq.retry-retry-overflow-redelay").message_count.should eq 1

          wait_for { q.get(no_ack: true) }.body_io.to_s.should eq "block"
          msg2 = wait_for(timeout: 5.seconds) { q.get(no_ack: true) }
          msg2.body_io.to_s.should eq "retry me"
        end
      end
    end

    it "should close the retry queue with the primary queue" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-delivery-limit"    => 3,
            "x-delayed-retry-min" => 60_000,
          })
          q = ch.queue("retry-closed-primary", args: args)
          q.publish_confirm "survive"

          msg = wait_for { q.get(no_ack: false) }
          msg.reject(requeue: true)
          wait_for { s.vhosts["/"].queue("amq.retry-retry-closed-primary").message_count == 1 }
          s.vhosts["/"].queue("retry-closed-primary").close

          retry_q = s.vhosts["/"].queue("amq.retry-retry-closed-primary")
          retry_q.closed?.should be_true
          retry_q.message_count.should eq 1
        end
      end
    end

    it "should requeue instantly if the retry queue store fails" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-delivery-limit"    => 3,
            "x-delayed-retry-min" => 60_000,
          })
          q = ch.queue("retry-store-error", args: args)
          retry_q = vhost.queue("amq.retry-retry-store-error")
          LavinMQ::AMQP::DelayedQueue::DelayedMessageStore.fail_next_push = true

          q.publish_confirm "x"
          msg = wait_for { q.get(no_ack: false) }
          msg.reject(requeue: true)

          msg2 = wait_for { q.get(no_ack: false) }
          msg2.body_io.to_s.should eq "x"
          retry_q.closed?.should be_false
          vhost.queue("retry-store-error").closed?.should be_false
          vhost.queue("amq.retry-retry-store-error").should be retry_q

          # The retry queue keeps working once it can store messages
          msg2.reject(requeue: true)
          wait_for { retry_q.message_count == 1 }
        end
      ensure
        LavinMQ::AMQP::DelayedQueue::DelayedMessageStore.fail_next_push = false
      end
    end

    it "should keep the retry queue open if its purge fails" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        with_channel(s) do |ch|
          q = ch.queue("retry-purge-error", args: AMQP::Client::Arguments.new({"x-delayed-retry-min" => 60_000}))
          retry_q = vhost.queue("amq.retry-retry-purge-error")
          q.publish_confirm "a"
          wait_for { q.get(no_ack: false) }.reject(requeue: true)
          wait_for { retry_q.message_count == 1 }
          LavinMQ::AMQP::DelayedQueue::DelayedMessageStore.fail_next_purge = true
          expect_raises(LavinMQ::MessageStore::Error) { retry_q.purge }
          retry_q.closed?.should be_false
          retry_q.purge.should eq 1
        end
      ensure
        LavinMQ::AMQP::DelayedQueue::DelayedMessageStore.fail_next_purge = false
      end
    end

    it "should release delayed messages again after the release loop stopped on a store error" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        with_channel(s) do |ch|
          q = ch.queue("retry-expire-error", args: AMQP::Client::Arguments.new({"x-delayed-retry-min" => 200}))
          retry_q = vhost.queue("amq.retry-retry-expire-error")
          LavinMQ::AMQP::DelayedRetryQueue.fail_next_expire = true
          q.publish_confirm "a"
          wait_for { q.get(no_ack: false) }.reject(requeue: true)
          wait_for { !retry_q.message_expire_fiber_active? }
          sleep 300.milliseconds
          retry_q.closed?.should be_false
          retry_q.message_count.should eq 1 # not released, and the loop didn't restart

          q.publish_confirm "b"
          wait_for { q.get(no_ack: false) }.reject(requeue: true)
          wait_for { vhost.queue("retry-expire-error").message_count == 2 }
        end
      ensure
        LavinMQ::AMQP::DelayedRetryQueue.fail_next_expire = false
      end
    end

    # A release can publish its copy to the primary queue after a store error
    # started to close the retry queue. Without its ack the retry queue that
    # replaces this one loads the message and releases it again.
    it "writes the ack of a message released while the retry queue closes" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          q = ch.queue("closing-retry", args: AMQP::Client::Arguments.new({"x-delayed-retry-min" => 60_000}))
          q.publish_confirm "x"
          msg = wait_for { q.get(no_ack: false) }
          msg.reject(requeue: true)
        end
        retry_q = s.vhosts["/"].queue("amq.retry-closing-retry")
        wait_for { retry_q.message_count == 1 }
        retry_q.spec_mark_closing
        retry_q.spec_shift_and_delete_moved
        retry_q.spec_close_store
        msg_dir = retry_q.@msg_store.@msg_dir
        reloaded = LavinMQ::AMQP::DelayedQueue::DelayedMessageStore.new(msg_dir, nil)
        begin
          reloaded.size.should eq 0
        ensure
          reloaded.close
        end
      end
    end

    # The close can come from another fiber, e.g. a store error in a publish,
    # while a release has published its copy but not yet written its ack
    it "waits for a release in flight before it closes the store" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        vhost.declare_exchange("closing-delayed", "x-delayed-message", true, false,
          arguments: LavinMQ::AMQP::Table.new({"x-delayed-type" => "direct"}))
        blocking = LavinMQ::AMQP::BlockingDeadLetterQueue.create(vhost, "closing-delayed-blocking")
        vhost.exchange("closing-delayed").bind(blocking, "rk", nil)
        delayed_q = vhost.queue("amq.delayed-closing-delayed")
        with_channel(s) do |ch|
          headers = AMQP::Client::Arguments.new({"x-delay" => 50})
          ch.basic_publish_confirm("x", "closing-delayed", "rk", props: AMQP::Client::Properties.new(headers: headers))
        end
        select
        when blocking.entered.receive
        when timeout(5.seconds)
          fail "the message was never released"
        end
        closed = Channel(Nil).new
        spawn { delayed_q.close; closed.close }
        select
        when closed.receive?
          fail "closed the store while a release was in flight"
        when timeout(200.milliseconds)
        end
        blocking.release.send(nil)
        closed.receive?
        msg_dir = delayed_q.@msg_store.@msg_dir
        reloaded = LavinMQ::AMQP::DelayedQueue::DelayedMessageStore.new(msg_dir, nil)
        begin
          reloaded.size.should eq 0
        ensure
          reloaded.close
        end
        blocking.message_count.should eq 1
      end
    end

    # The delete holds the vhost's definitions lock, and the release can need
    # it, e.g. when its route deletes a non-durable queue after a store error
    it "deletes a delayed queue without waiting for a release in flight" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        vhost.declare_exchange("deleting-delayed", "x-delayed-message", true, false,
          arguments: LavinMQ::AMQP::Table.new({"x-delayed-type" => "direct"}))
        blocking = LavinMQ::AMQP::BlockingDeadLetterQueue.create(vhost, "deleting-delayed-blocking")
        vhost.exchange("deleting-delayed").bind(blocking, "rk", nil)
        delayed_q = vhost.queue("amq.delayed-deleting-delayed")
        with_channel(s) do |ch|
          headers = AMQP::Client::Arguments.new({"x-delay" => 50})
          ch.basic_publish_confirm("x", "deleting-delayed", "rk", props: AMQP::Client::Properties.new(headers: headers))
        end
        select
        when blocking.entered.receive
        when timeout(5.seconds)
          fail "the message was never released"
        end
        deleted = Channel(Nil).new
        spawn { vhost.delete_exchange("deleting-delayed"); deleted.close }
        begin
          select
          when deleted.receive?
          when timeout(5.seconds)
            fail "the delete waited for the release in flight"
          end
        ensure
          blocking.release.send(nil)
        end
        delayed_q.state.should eq LavinMQ::QueueState::Deleted
        Dir.exists?(delayed_q.@msg_store.@msg_dir).should be_false
        wait_for { blocking.message_count == 1 }
      end
    end

    # The close of the retry queue waits for the release, and the release can
    # need the definitions lock, which a delete holds while it takes the lock
    it "closes the retry queue outside the retry queue lock" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        vhost.declare_exchange("closing-primary-dlx", "direct", false, false)
        blocking = LavinMQ::AMQP::BlockingDeadLetterQueue.create(vhost, "closing-primary-blocking")
        vhost.exchange("closing-primary-dlx").bind(blocking, "blocked", nil)
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-delayed-retry-min"       => 10_000,
            "x-dead-letter-exchange"    => "closing-primary-dlx",
            "x-dead-letter-routing-key" => "blocked",
          })
          q = ch.queue("closing-primary", args: args)
          q.publish_confirm "expire me", props: AMQP::Client::Properties.new(expiration: "300")
          wait_for { q.get(no_ack: false) }.reject(requeue: true)
        end
        select
        when blocking.entered.receive
        when timeout(5.seconds)
          fail "the message was never dead-lettered"
        end
        primary = vhost.queue("closing-primary")
        closed = Channel(Nil).new
        spawn { primary.close; closed.close }
        begin
          wait_for { primary.closed? }
          locked = Channel(Nil).new
          spawn { primary.@retry_queue_lock.synchronize { }; locked.close }
          select
          when locked.receive?
          when timeout(5.seconds)
            fail "held the retry queue lock while the release was in flight"
          end
        ensure
          blocking.release.send(nil)
        end
        closed.receive?
        wait_for { blocking.message_count == 1 }
      end
    end

    # A segment of 4 schema bytes and one record whose property flags are
    # invalid, so it can't be decoded
    corrupt_segment = -> do
      io = IO::Memory.new
      io.write_bytes LavinMQ::Schema::VERSION
      io.write_bytes 1_i64 # timestamp
      io.write_byte 0_u8   # exchange name
      io.write_byte 0_u8   # routing key
      io.write_bytes 1_u16 # property flags, an invalid bit
      io.write_bytes 0_u64 # body size
      io.to_slice
    end

    it "closes a delayed store with a record that can't be decoded, instead of indexing it" do
      with_datadir do |dir|
        File.write(File.join(dir, "msgs.0000000001"), corrupt_segment.call)
        LavinMQ::AMQP::DelayedQueue::DelayedMessageStore.new(dir, nil).closed.should be_true
      end
    end

    it "closes a delayed store when indexing finds a record that can't be decoded" do
      with_datadir do |dir|
        # With a meta file the load trusts its count and doesn't read the records
        File.write(File.join(dir, "msgs.0000000001"), corrupt_segment.call)
        File.open(File.join(dir, "meta.0000000001"), "w", &.write_bytes(1_u32))
        File.write(File.join(dir, "msgs.0000000002"), "\x04\x00\x00\x00")
        LavinMQ::AMQP::DelayedQueue::DelayedMessageStore.new(dir, nil).closed.should be_true
      end
    end

    it "starts with a retry queue whose segment is corrupt, and closes that queue" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          q = ch.queue("corrupt-retry", args: AMQP::Client::Arguments.new({"x-delayed-retry-min" => 60_000}))
          q.publish_confirm "x"
          msg = wait_for { q.get(no_ack: false) }
          msg.reject(requeue: true)
          wait_for { s.vhosts["/"].queue("amq.retry-corrupt-retry").message_count == 1 }
        end
        segment = File.join(s.vhosts["/"].queue("amq.retry-corrupt-retry").@data_dir, "msgs.0000000001")
        File.open(segment, "r+", &.write_bytes(9_u32)) # an unsupported schema version
        restart_server(s)
        s.vhosts["/"].queue("amq.retry-corrupt-retry").closed?.should be_true
        with_channel(s) { |ch| ch.queue("other").publish_confirm("x").should be_true }
      end
    end

    it "should not leak mmaps when the retry queue store can't be loaded" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({"x-delayed-retry-min" => 60_000})
          q = ch.queue("retry-load-error", args: args)
          retry_q = s.vhosts["/"].queue("amq.retry-retry-load-error")
          retry_q.close
          # MFile can't open a directory, so recreating the retry queue fails
          Dir.mkdir(File.join(retry_q.@msg_store.@msg_dir, "msgs.0000000002"))

          q.publish_confirm "msg"
          msg = wait_for { q.get(no_ack: false) }
          msg.reject(requeue: true)
          count_before = MFile.mmap_count
          5.times do
            msg = wait_for { q.get(no_ack: false) }
            msg.reject(requeue: true)
          end
          MFile.mmap_count.should eq count_before

          q.delete
          s.vhosts["/"].queue?("amq.retry-retry-load-error").should be_nil
        end
      end
    end

    it "should purge delayed messages with the queue" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-delivery-limit"    => 3,
            "x-delayed-retry-min" => 60_000,
          })
          q = ch.queue("retry-purge", args: args)
          q.publish_confirm "delayed"
          msg = wait_for { q.get(no_ack: false) }
          msg.reject(requeue: true)
          wait_for { s.vhosts["/"].queue("amq.retry-retry-purge").message_count == 1 }

          Log.capture("lmq.*", :warn) do |logs|
            purged = ch.queue_purge("retry-purge")
            purged[:message_count].should eq 1
            logs.empty
          end
          s.vhosts["/"].queue("amq.retry-retry-purge").message_count.should eq 0
        end
      end
    end

    it "should refuse a multiplier above Int32 max" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-delayed-retry-min"        => 1000,
            "x-delayed-retry-multiplier" => 3_000_000_000_i64,
          })
          expect_raises(AMQP::Client::Channel::ClosedException, /PRECONDITION_FAILED/) do
            ch.queue("retry-multiplier-overflow", args: args)
          end
        end
      end
    end

    it "should reject a queue name that leaves no room for the retry queue prefix" do
      with_amqp_server do |s|
        name = "q" * 246
        with_channel(s) do |ch|
          expect_raises(AMQP::Client::Channel::ClosedException, /too long/) do
            ch.queue(name, args: AMQP::Client::Arguments.new({"x-delayed-retry-min" => 1000}))
          end
        end
        s.vhosts["/"].queue?(name).should be_nil
        Dir.exists?(File.join(s.vhosts["/"].data_dir, Digest::SHA1.hexdigest(name))).should be_false
      end
    end

    it "should reject combining retry with message deduplication" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          expect_raises(AMQP::Client::Channel::ClosedException, /x-message-deduplication/) do
            args = AMQP::Client::Arguments.new({
              "x-delivery-limit"        => 3,
              "x-delayed-retry-min"     => 1000,
              "x-message-deduplication" => true,
            })
            ch.queue("retry-dedup", args: args)
          end
        end
      end
    end

    it "should clamp the retry delay instead of overflowing" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-delivery-limit"           => 100,
            "x-delayed-retry-min"        => 1,
            "x-delayed-retry-multiplier" => 4,
            "x-delayed-retry-max"        => 1,
          })
          q = ch.queue("retry-overflow", args: args)
          q.publish_confirm "overflow test"

          70.times do
            msg = wait_for(timeout: 5.seconds) { q.get(no_ack: false) }
            msg.reject(requeue: true)
          end

          ch.closed?.should be_false
          wait_for(timeout: 5.seconds) { q.get(no_ack: false) }.should_not be_nil
        end
      end
    end

    it "should give a retry-enabled dead letter queue its own retry budget" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          dlq_args = AMQP::Client::Arguments.new({
            "x-delivery-limit"    => 3,
            "x-delayed-retry-min" => 60_000,
          })
          dlq = ch.queue("retry-chain-dlq", args: dlq_args)
          args = AMQP::Client::Arguments.new({
            "x-delivery-limit"          => 1,
            "x-delayed-retry-min"       => 1,
            "x-dead-letter-exchange"    => "",
            "x-dead-letter-routing-key" => "retry-chain-dlq",
          })
          q = ch.queue("retry-chain", args: args)
          q.publish_confirm "chain"

          2.times do
            msg = wait_for(timeout: 5.seconds) { q.get(no_ack: false) }
            msg.reject(requeue: true)
          end

          msg = wait_for(timeout: 5.seconds) { dlq.get(no_ack: false) }
          msg.properties.headers.try(&.["x-delivery-count"]?).should be_nil
          msg.reject(requeue: true)

          wait_for { s.vhosts["/"].queue("amq.retry-retry-chain-dlq").message_count == 1 }
        end
      end
    end

    it "should respect an explicit x-delivery-limit of 0" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-delivery-limit"    => 0,
            "x-delayed-retry-min" => 1,
          })
          q = ch.queue("retry-limit-zero", args: args)
          ch.default_exchange.publish_confirm("limit zero", q.name)

          msg = wait_for { q.get(no_ack: false) }
          msg.reject(requeue: true)

          sleep 50.milliseconds
          s.vhosts["/"].queue("retry-limit-zero").message_count.should eq 0
        end
      end
    end
  end

  describe "Lifecycle and locking" do
    it "deletes a queue under the definitions lock while a nack multiple rejects into a closed retry queue" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        with_channel(s) do |ch|
          ch.prefetch(5)
          q = ch.queue("prim", args: AMQP::Client::Arguments.new({"x-delayed-retry-min" => 60_000}))
          20.times { q.publish_confirm "x" }
          vhost.queue("amq.retry-prim").close
          tags = Channel(UInt64).new(100)
          q.subscribe(no_ack: false) { |m| tags.send m.delivery_tag }
          5.times { tags.receive }
          deleted = Channel(Nil).new
          spawn do
            vhost.synchronize_definitions do
              ch.basic_nack(5u64, requeue: true, multiple: true)
              sleep 100.milliseconds
              vhost.delete_queue("prim")
            end
            deleted.close
          end
          select
          when deleted.receive?
          when timeout(5.seconds)
            fail "the queue delete deadlocked with the nack"
          end
          vhost.queue?("amq.retry-prim").should be_nil
        end
      end
    end

    it "deletes a queue under the definitions lock while a nack multiple rejects into a failing non-durable retry queue" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        with_channel(s) do |ch|
          ch.prefetch(5)
          q = ch.queue("prim", durable: false, args: AMQP::Client::Arguments.new({"x-delayed-retry-min" => 60_000}))
          20.times { q.publish_confirm "x" }
          tags = Channel(UInt64).new(100)
          q.subscribe(no_ack: false) { |m| tags.send m.delivery_tag }
          5.times { tags.receive }
          other = ch.queue("other", durable: false)
          other.subscribe(no_ack: false) { |_| }
          LavinMQ::AMQP::DelayedQueue::DelayedMessageStore.fail_next_push = true
          deleted = Channel(Nil).new
          spawn do
            vhost.synchronize_definitions do
              ch.basic_nack(5u64, requeue: true, multiple: true)
              sleep 100.milliseconds
              with_channel(s) { |ch2| ch2.basic_publish_confirm("y", "", "other") }
              sleep 100.milliseconds
              vhost.delete_queue("other")
            end
            deleted.close
          end
          select
          when deleted.receive?
          when timeout(5.seconds)
            fail "the queue delete deadlocked with the nack"
          end
          retry_q = vhost.queue("amq.retry-prim")
          retry_q.closed?.should be_false
          wait_for { retry_q.message_count == 4 } # the first was requeued instantly
        end
      ensure
        LavinMQ::AMQP::DelayedQueue::DelayedMessageStore.fail_next_push = false
      end
    end

    it "requeues the rest of a nack multiple when a reject raises" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        vhost.register_queue(LavinMQ::AMQP::RejectFailingQueue.create(vhost, "a"))
        with_channel(s) do |ch|
          b = ch.queue("b")
          ch.basic_publish_confirm "a", "", "a"
          2.times { b.publish_confirm "b" }
          wait_for { b.get(no_ack: false) }
          wait_for { ch.basic_get("a", no_ack: false) }
          last = wait_for { b.get(no_ack: false) }
          ch.basic_nack(last.delivery_tag, requeue: true, multiple: true)
          wait_for { ch.closed? }
        end
        wait_for { vhost.queue("b").message_count == 2 }
      end
    end

    it "restarts the retry queue with its primary queue" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        with_channel(s) do |ch|
          q = ch.queue("prim", args: AMQP::Client::Arguments.new({"x-delayed-retry-min" => 1000}))
          q.publish_confirm "x"
          msg = wait_for { q.get(no_ack: false) }
          msg.reject(requeue: true)
          retry_q = vhost.queue("amq.retry-prim")
          wait_for { retry_q.message_count == 1 }
          prim = vhost.queue("prim")
          prim.close
          retry_q.closed?.should be_true
          retry_q.message_count.should eq 1 # still delayed when it closed
          prim.restart!.should be_true
          restarted = vhost.queue("amq.retry-prim")
          restarted.should_not be retry_q
          restarted.closed?.should be_false
          wait_for { prim.message_count == 1 }
        end
      end
    end
  end

  describe "Basic retry" do
    it "should not retry on reject with requeue=false" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          dlq = ch.queue("retry-no-requeue-dlq")
          args = AMQP::Client::Arguments.new({
            "x-delivery-limit"          => 3,
            "x-delayed-retry-min"       => 1,
            "x-dead-letter-exchange"    => "",
            "x-dead-letter-routing-key" => "retry-no-requeue-dlq",
          })
          q = ch.queue("retry-no-requeue", args: args)
          ch.default_exchange.publish_confirm("reject test", q.name)

          msg = wait_for { q.get(no_ack: false) }
          msg.reject(requeue: false)

          dlq_msg = wait_for { dlq.get(no_ack: true) }
          dlq_msg.body_io.to_s.should eq "reject test"
        end
      end
    end

    it "should preserve message body and properties through retry" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-delivery-limit"    => 3,
            "x-delayed-retry-min" => 1,
          })
          q = ch.queue("retry-props", args: args)
          props = AMQP::Client::Properties.new(
            content_type: "application/json",
            correlation_id: "abc-123",
            headers: AMQ::Protocol::Table.new({"x-custom" => "value"})
          )
          ch.default_exchange.publish_confirm("body", q.name, props: props)

          msg = wait_for { q.get(no_ack: false) }
          msg.reject(requeue: true)

          msg2 = wait_for { q.get(no_ack: true) }
          msg2.body_io.to_s.should eq "body"
          msg2.properties.content_type.should eq "application/json"
          msg2.properties.correlation_id.should eq "abc-123"
          headers = msg2.properties.headers.should_not be_nil
          headers["x-custom"].should eq "value"
        end
      end
    end

    it "should flag a retried message as redelivered" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-delivery-limit"    => 3,
            "x-delayed-retry-min" => 1,
          })
          q = ch.queue("retry-redelivered", args: args)
          q.publish_confirm "retry me"

          msg = wait_for { q.get(no_ack: false) }
          msg.redelivered.should be_false
          msg.reject(requeue: true)

          msg2 = wait_for { q.get(no_ack: false) }
          msg2.redelivered.should be_true
          msg2.reject(requeue: true)

          msg3 = wait_for { q.get(no_ack: true) }
          msg3.redelivered.should be_true
        end
      end
    end

    it "should also trigger retry on nack(requeue=true)" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-delivery-limit"    => 3,
            "x-delayed-retry-min" => 200,
          })
          q = ch.queue("retry-nack-trigger", args: args)
          ch.default_exchange.publish_confirm("nack body", q.name)

          msg = wait_for { q.get(no_ack: false) }
          msg.nack(requeue: true)
          start = Time.instant
          msg2 = wait_for(timeout: 5.seconds) { q.get(no_ack: true) }
          delay = Time.instant - start
          msg2.body_io.to_s.should eq "nack body"
          delay.should be >= 180.milliseconds
          delay.should be < 500.milliseconds
        end
      end
    end

    it "should not delay broker-initiated requeue on channel close" do
      with_amqp_server do |s|
        args = AMQP::Client::Arguments.new({
          "x-delivery-limit"    => 3,
          "x-delayed-retry-min" => 60000,
        })
        with_channel(s) do |ch|
          q = ch.queue("retry-close-bypass", args: args)
          ch.default_exchange.publish_confirm("close body", q.name)
          msg = wait_for { q.get(no_ack: false) }
          msg.body_io.to_s.should eq "close body"
        end
        # channel/connection closed without ack — unacked msg should requeue instantly, not into retry queue
        with_channel(s) do |ch|
          q = ch.queue("retry-close-bypass", args: args)
          start = Time.instant
          msg = wait_for(timeout: 2.seconds) { q.get(no_ack: true) }
          delay = Time.instant - start
          msg.body_io.to_s.should eq "close body"
          delay.should be < 500.milliseconds
          s.vhosts["/"].queue("amq.retry-retry-close-bypass").message_count.should eq 0
        end
      end
    end
  end

  describe "Exponential backoff" do
    it "should default to linear backoff when multiplier is omitted" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-delivery-limit"    => 5,
            "x-delayed-retry-min" => 200,
          })
          q = ch.queue("retry-linear", args: args)
          ch.default_exchange.publish_confirm("msg", q.name)

          delays = [] of Time::Span
          3.times do
            start = Time.instant
            msg = wait_for(timeout: 5.seconds) { q.get(no_ack: false) }
            delays << Time.instant - start
            msg.reject(requeue: true)
          end

          # Linear: delay = min × delivery_count → 200ms, 400ms, ...
          delays[1].should be >= 180.milliseconds
          delays[1].should be < 350.milliseconds
          delays[2].should be >= 380.milliseconds
          delays[2].should be < 600.milliseconds
        end
      end
    end

    it "should give constant delay when multiplier = 1" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-delivery-limit"           => 5,
            "x-delayed-retry-min"        => 200,
            "x-delayed-retry-multiplier" => 1,
          })
          q = ch.queue("retry-constant", args: args)
          ch.default_exchange.publish_confirm("msg", q.name)

          delays = [] of Time::Span
          3.times do
            start = Time.instant
            msg = wait_for(timeout: 5.seconds) { q.get(no_ack: false) }
            delays << Time.instant - start
            msg.reject(requeue: true)
          end

          # All retries should wait ~200ms (constant), not grow
          delays[1].should be >= 180.milliseconds
          delays[1].should be < 350.milliseconds
          delays[2].should be >= 180.milliseconds
          delays[2].should be < 350.milliseconds
        end
      end
    end

    it "should apply exponential delay when multiplier > 1" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-delivery-limit"           => 5,
            "x-delayed-retry-min"        => 200,
            "x-delayed-retry-multiplier" => 2,
          })
          q = ch.queue("retry-backoff", args: args)
          ch.default_exchange.publish_confirm("msg", q.name)

          delays = [] of Time::Span
          3.times do
            start = Time.instant
            msg = wait_for(timeout: 10.seconds) { q.get(no_ack: false) }
            delays << Time.instant - start
            msg.reject(requeue: true)
          end

          delays[1].should be >= 180.milliseconds
          delays[1].should be < 600.milliseconds
          delays[2].should be >= 380.milliseconds
          delays[2].should be < 1000.milliseconds
        end
      end
    end

    it "should cap delay at x-delayed-retry-max" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-delivery-limit"           => 5,
            "x-delayed-retry-min"        => 100,
            "x-delayed-retry-multiplier" => 2,
            "x-delayed-retry-max"        => 250,
          })
          q = ch.queue("retry-cap", args: args)
          ch.default_exchange.publish_confirm("msg", q.name)

          msg = wait_for { q.get(no_ack: false) }
          msg.reject(requeue: true)
          start = Time.instant
          msg = wait_for(timeout: 5.seconds) { q.get(no_ack: false) }
          delay1 = Time.instant - start
          delay1.should be >= 80.milliseconds
          delay1.should be < 300.milliseconds

          msg.reject(requeue: true)
          start = Time.instant
          msg = wait_for(timeout: 5.seconds) { q.get(no_ack: false) }
          delay2 = Time.instant - start
          delay2.should be >= 180.milliseconds
          delay2.should be < 500.milliseconds

          msg.reject(requeue: true)
          start = Time.instant
          msg = wait_for(timeout: 5.seconds) { q.get(no_ack: false) }
          delay3 = Time.instant - start
          delay3.should be >= 230.milliseconds
          delay3.should be < 600.milliseconds

          msg.ack
        end
      end
    end
  end

  describe "Delivery limit exhausted" do
    it "should dead-letter after delivery limit" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          dlq = ch.queue("retry-dlq")
          args = AMQP::Client::Arguments.new({
            "x-delivery-limit"          => 2,
            "x-delayed-retry-min"       => 1,
            "x-dead-letter-exchange"    => "",
            "x-dead-letter-routing-key" => "retry-dlq",
          })
          q = ch.queue("retry-exhaust-dlx", args: args)
          ch.default_exchange.publish_confirm("dlx test", q.name)

          3.times do
            msg = wait_for { q.get(no_ack: false) }
            msg.reject(requeue: true)
          end

          dlq_msg = wait_for { dlq.get(no_ack: true) }
          dlq_msg.body_io.to_s.should eq "dlx test"
          headers = dlq_msg.properties.headers.should_not be_nil
          headers["x-death"].should_not be_nil
        end
      end
    end

    it "should discard after delivery limit when no DLX" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-delivery-limit"    => 2,
            "x-delayed-retry-min" => 1,
          })
          q = ch.queue("retry-exhaust-discard", args: args)
          ch.default_exchange.publish_confirm("discard test", q.name)

          3.times do
            msg = wait_for { q.get(no_ack: false) }
            msg.reject(requeue: true)
          end

          sleep 50.milliseconds
          q.get(no_ack: true).should be_nil
          s.vhosts["/"].queue("retry-exhaust-discard").message_count.should eq 0
        end
      end
    end

    it "should keep retrying at capped delay until x-delivery-limit ends it" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          dlq = ch.queue("retry-cap-dlq")
          args = AMQP::Client::Arguments.new({
            "x-delivery-limit"           => 4,
            "x-delayed-retry-min"        => 1,
            "x-delayed-retry-multiplier" => 2,
            "x-delayed-retry-max"        => 3,
            "x-dead-letter-exchange"     => "",
            "x-dead-letter-routing-key"  => "retry-cap-dlq",
          })
          q = ch.queue("retry-cap-pure", args: args)
          ch.default_exchange.publish_confirm("cap test", q.name)

          # Cap is a pure clamp; retries keep going at the capped delay until
          # x-delivery-limit terminates. With limit=4 the message is dead-lettered
          # on the 5th delivery attempt.
          5.times do
            msg = wait_for(timeout: 5.seconds) { q.get(no_ack: false) }
            msg.reject(requeue: true)
          end

          dlq_msg = wait_for { dlq.get(no_ack: true) }
          dlq_msg.body_io.to_s.should eq "cap test"
        end
      end
    end
  end

  describe "Cleanup" do
    it "should not error when primary queue is deleted while messages are in retry queue" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-delivery-limit"    => 3,
            "x-delayed-retry-min" => 60000,
          })
          q = ch.queue("retry-delete-pending", args: args)
          ch.default_exchange.publish_confirm("pending msg", q.name)

          msg = wait_for { q.get(no_ack: false) }
          msg.reject(requeue: true)

          sleep 10.milliseconds
          s.vhosts["/"].queue("amq.retry-retry-delete-pending").message_count.should eq 1

          q.delete
          s.vhosts["/"].queue?("amq.retry-retry-delete-pending").should be_nil
          s.vhosts["/"].queue?("retry-delete-pending").should be_nil
        end
      end
    end

    it "should release the primary queue's segment after routing a message to the retry queue" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-delivery-limit"    => 3,
            "x-delayed-retry-min" => 60_000,
          })
          q = ch.queue("retry-lease", args: args)
          q.publish_confirm "delayed"
          vhost = s.vhosts["/"]
          segment = vhost.queue(q.name).@msg_store.@segments.first_value

          msg = wait_for { q.get(no_ack: false) }
          msg.reject(requeue: true)
          wait_for { vhost.queue("amq.retry-retry-lease").message_count == 1 }

          vhost.delete_queue(q.name)
          segment.closed?.should be_true
        end
      end
    end
  end

  describe "Durability" do
    it "should not persist the retry queue in the definitions file" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-delivery-limit"    => 3,
            "x-delayed-retry-min" => 1000,
          })
          ch.queue("retry-defs", durable: true, args: args)
          ch.queue("retry-defs-tmp", durable: true).delete
        end

        restart_server(s)

        defs = File.read(File.join(s.vhosts["/"].data_dir, "definitions.amqp"))
        defs.includes?("amq.retry-retry-defs").should be_false
        s.vhosts["/"].queue("amq.retry-retry-defs").should be_a(LavinMQ::AMQP::DelayedRetryQueue)
      end
    end

    it "should survive broker restart" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-delivery-limit"    => 3,
            "x-delayed-retry-min" => 100,
          })
          q = ch.queue("retry-durable", durable: true, args: args)
          ch.default_exchange.publish_confirm("persist", q.name)
          msg = wait_for { q.get(no_ack: false) }
          msg.reject(requeue: true)
          sleep 10.milliseconds
          s.vhosts["/"].queue("amq.retry-retry-durable").message_count.should eq 1
        end

        restart_server(s)

        s.vhosts["/"].queue?("amq.retry-retry-durable").should_not be_nil
        with_channel(s) do |ch|
          q = ch.queue("retry-durable", durable: true, args: AMQP::Client::Arguments.new({
            "x-delivery-limit"    => 3,
            "x-delayed-retry-min" => 100,
          }))
          msg = wait_for { q.get(no_ack: true) }
          msg.body_io.to_s.should eq "persist"
        end
      end
    end
  end
end
