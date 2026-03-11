require "./spec_helper"
require "./../src/lavinmq/amqp/queue"

module QueueRetrySpec
  QUEUE_NAME = "retry-q"
  DLQ_NAME   = "retry-dlq"

  describe "Queue Retry" do
    it "should retry on reject with requeue=false and set x-retry-count header" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-retry-max-count" => 3,
            "x-retry-delay"     => 100,
          })
          q = ch.queue(QUEUE_NAME, args: args)
          ch.default_exchange.publish_confirm("msg1", q.name)

          msg = wait_for { q.get(no_ack: false) }
          msg.not_nil!.body_io.to_s.should eq "msg1"
          msg.not_nil!.reject(requeue: false)

          # Message should reappear after retry delay with x-retry-count: 1
          msg2 = wait_for { q.get(no_ack: true) }
          msg2.not_nil!.body_io.to_s.should eq "msg1"
          headers = msg2.not_nil!.properties.headers.should_not be_nil
          headers["x-retry-count"].should eq 1
        end
      end
    end

    it "should increment x-retry-count on multiple retries" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-retry-max-count" => 3,
            "x-retry-delay"     => 100,
          })
          q = ch.queue(QUEUE_NAME, args: args)
          ch.default_exchange.publish_confirm("msg1", q.name)

          # First reject
          msg = wait_for { q.get(no_ack: false) }
          msg.not_nil!.reject(requeue: false)

          # First retry - count should be 1
          msg = wait_for { q.get(no_ack: false) }
          headers = msg.not_nil!.properties.headers.should_not be_nil
          headers["x-retry-count"].should eq 1
          msg.not_nil!.reject(requeue: false)

          # Second retry - count should be 2
          msg = wait_for { q.get(no_ack: false) }
          headers = msg.not_nil!.properties.headers.should_not be_nil
          headers["x-retry-count"].should eq 2
        end
      end
    end

    it "should dead letter after max retries exhausted with DLX" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-retry-max-count"         => 1,
            "x-retry-delay"             => 100,
            "x-dead-letter-exchange"    => "",
            "x-dead-letter-routing-key" => DLQ_NAME,
          })
          q = ch.queue(QUEUE_NAME, args: args)
          dlq = ch.queue(DLQ_NAME)
          ch.default_exchange.publish_confirm("msg1", q.name)

          # First reject -> goes to retry queue
          msg = wait_for { q.get(no_ack: false) }
          msg.not_nil!.reject(requeue: false)

          # After retry, message comes back with x-retry-count: 1
          msg = wait_for { q.get(no_ack: false) }
          headers = msg.not_nil!.properties.headers.should_not be_nil
          headers["x-retry-count"].should eq 1
          # Second reject -> max retries exhausted -> DLX
          msg.not_nil!.reject(requeue: false)

          # Should appear in DLQ
          dlq_msg = wait_for { dlq.get(no_ack: true) }
          dlq_msg.not_nil!.body_io.to_s.should eq "msg1"
        end
      end
    end

    it "should discard message when max retries exhausted without DLX" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-retry-max-count" => 1,
            "x-retry-delay"     => 100,
          })
          q = ch.queue(QUEUE_NAME, args: args)
          ch.default_exchange.publish_confirm("msg1", q.name)

          # First reject -> retry
          msg = wait_for { q.get(no_ack: false) }
          msg.not_nil!.reject(requeue: false)

          # After retry
          msg = wait_for { q.get(no_ack: false) }
          headers = msg.not_nil!.properties.headers.should_not be_nil
          headers["x-retry-count"].should eq 1
          # Second reject -> exhausted, no DLX -> discard
          msg.not_nil!.reject(requeue: false)

          sleep 0.2.seconds
          q.message_count.should eq 0
        end
      end
    end

    it "should delete retry queues when primary queue is deleted" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-retry-max-count" => 3,
            "x-retry-delay"     => 100,
          })
          q = ch.queue(QUEUE_NAME, args: args)

          # Verify retry queues exist
          s.vhosts["/"].queues.has_key?("#{QUEUE_NAME}.x-retry.0").should be_true
          s.vhosts["/"].queues.has_key?("#{QUEUE_NAME}.x-retry.1").should be_true
          s.vhosts["/"].queues.has_key?("#{QUEUE_NAME}.x-retry.2").should be_true

          q.delete
          sleep 0.1.seconds

          # Retry queues should be gone
          s.vhosts["/"].queues.has_key?("#{QUEUE_NAME}.x-retry.0").should be_false
          s.vhosts["/"].queues.has_key?("#{QUEUE_NAME}.x-retry.1").should be_false
          s.vhosts["/"].queues.has_key?("#{QUEUE_NAME}.x-retry.2").should be_false
        end
      end
    end

    it "should be idempotent on redeclare with same args" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-retry-max-count" => 3,
            "x-retry-delay"     => 100,
          })
          ch.queue(QUEUE_NAME, args: args)
          # Redeclare with same args should not error
          ch.queue(QUEUE_NAME, args: args)
        end
      end
    end

    it "should not allow clients to consume from internal retry queues" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-retry-max-count" => 3,
            "x-retry-delay"     => 100,
          })
          ch.queue(QUEUE_NAME, args: args)

          expect_raises(AMQP::Client::Channel::ClosedException, /ACCESS_REFUSED/) do
            ch.queue_declare("#{QUEUE_NAME}.x-retry.0", passive: true)
          end
        end
      end
    end

    it "should respect x-retry-delay-multiplier" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-retry-max-count"        => 3,
            "x-retry-delay"            => 100,
            "x-retry-delay-multiplier" => 3,
          })
          q = ch.queue(QUEUE_NAME, args: args)

          # Verify TTLs: 100, 300, 900
          vhost = s.vhosts["/"]
          rq0 = vhost.queues["#{QUEUE_NAME}.x-retry.0"]
          rq1 = vhost.queues["#{QUEUE_NAME}.x-retry.1"]
          rq2 = vhost.queues["#{QUEUE_NAME}.x-retry.2"]
          rq0.arguments["x-message-ttl"].should eq 100
          rq1.arguments["x-message-ttl"].should eq 300
          rq2.arguments["x-message-ttl"].should eq 900

          q.delete
        end
      end
    end

    it "should respect x-retry-max-delay" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-retry-max-count" => 3,
            "x-retry-delay"     => 100,
            "x-retry-max-delay" => 250,
          })
          q = ch.queue(QUEUE_NAME, args: args)

          # TTLs should be: 100, 200, 250 (capped)
          vhost = s.vhosts["/"]
          rq0 = vhost.queues["#{QUEUE_NAME}.x-retry.0"]
          rq1 = vhost.queues["#{QUEUE_NAME}.x-retry.1"]
          rq2 = vhost.queues["#{QUEUE_NAME}.x-retry.2"]
          rq0.arguments["x-message-ttl"].should eq 100
          rq1.arguments["x-message-ttl"].should eq 200
          rq2.arguments["x-message-ttl"].should eq 250

          q.delete
        end
      end
    end

    it "should not retry when requeue=true" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-retry-max-count" => 3,
            "x-retry-delay"     => 100,
          })
          q = ch.queue(QUEUE_NAME, args: args)
          ch.default_exchange.publish_confirm("msg1", q.name)

          msg = wait_for { q.get(no_ack: false) }
          msg.not_nil!.reject(requeue: true)

          # Message should be requeued normally, not retried
          msg2 = wait_for { q.get(no_ack: true) }
          msg2.not_nil!.body_io.to_s.should eq "msg1"
          msg2.not_nil!.properties.headers.try(&.["x-retry-count"]?).should be_nil
        end
      end
    end
  end
end
