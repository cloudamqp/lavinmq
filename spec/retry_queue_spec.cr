require "./spec_helper"

describe "Retry Queue" do
  describe "Scaffold" do
    it "should create internal retry queue when x-retry-delay is set" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-delivery-limit" => 3,
            "x-retry-delay"    => 1000,
          })
          ch.queue("retry-test", args: args)
          retry_q = s.vhosts["/"].queues["amq.retry-retry-test"]?
          retry_q.should_not be_nil
          retry_q.not_nil!.internal?.should be_true
        end
      end
    end

    it "should not create retry queue when x-retry-delay is not set" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({"x-delivery-limit" => 3})
          ch.queue("no-retry-test", args: args)
          s.vhosts["/"].queues["amq.retry-no-retry-test"]?.should be_nil
        end
      end
    end

    it "should reject negative x-retry-delay" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          expect_raises(AMQP::Client::Channel::ClosedException) do
            args = AMQP::Client::Arguments.new({
              "x-delivery-limit" => 3,
              "x-retry-delay"    => -1,
            })
            ch.queue("bad-retry", args: args)
          end
        end
      end
    end

    it "should reject non-integer x-retry-delay" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          expect_raises(AMQP::Client::Channel::ClosedException) do
            args = AMQP::Client::Arguments.new({
              "x-delivery-limit" => 3,
              "x-retry-delay"    => "bad",
            })
            ch.queue("bad-retry", args: args)
          end
        end
      end
    end
  end

  describe "Basic retry" do
    it "should delay requeue on nack with requeue=true" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-delivery-limit" => 3,
            "x-retry-delay"    => 1,
          })
          q = ch.queue("retry-basic", args: args)
          ch.default_exchange.publish_confirm("test body", q.name)

          msg = wait_for { q.get(no_ack: false) }
          msg.body_io.to_s.should eq "test body"
          msg.nack(requeue: true)

          msg2 = wait_for { q.get(no_ack: true) }
          msg2.body_io.to_s.should eq "test body"
        end
      end
    end

    it "should not retry on nack with requeue=false" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          dlq = ch.queue("retry-no-requeue-dlq")
          args = AMQP::Client::Arguments.new({
            "x-delivery-limit"          => 3,
            "x-retry-delay"             => 1,
            "x-dead-letter-exchange"    => "",
            "x-dead-letter-routing-key" => "retry-no-requeue-dlq",
          })
          q = ch.queue("retry-no-requeue", args: args)
          ch.default_exchange.publish_confirm("reject test", q.name)

          msg = wait_for { q.get(no_ack: false) }
          msg.nack(requeue: false)

          dlq_msg = wait_for { dlq.get(no_ack: true) }
          dlq_msg.body_io.to_s.should eq "reject test"
        end
      end
    end

    it "should preserve message body and properties through retry" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-delivery-limit" => 3,
            "x-retry-delay"    => 1,
          })
          q = ch.queue("retry-props", args: args)
          props = AMQP::Client::Properties.new(
            content_type: "application/json",
            correlation_id: "abc-123",
            headers: AMQ::Protocol::Table.new({"x-custom" => "value"})
          )
          ch.default_exchange.publish_confirm("body", q.name, props: props)

          msg = wait_for { q.get(no_ack: false) }
          msg.nack(requeue: true)

          msg2 = wait_for { q.get(no_ack: true) }
          msg2.body_io.to_s.should eq "body"
          msg2.properties.content_type.should eq "application/json"
          msg2.properties.correlation_id.should eq "abc-123"
          headers = msg2.properties.headers.should_not be_nil
          headers["x-custom"].should eq "value"
        end
      end
    end

    it "should instant requeue without x-retry-delay" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({"x-delivery-limit" => 3})
          q = ch.queue("retry-no-delay", args: args)
          ch.default_exchange.publish_confirm("instant", q.name)

          msg = wait_for { q.get(no_ack: false) }
          msg.nack(requeue: true)

          msg2 = wait_for { q.get(no_ack: true) }
          msg2.body_io.to_s.should eq "instant"
        end
      end
    end
  end

  describe "Exponential backoff" do
    it "should apply exponential delay with default multiplier" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-delivery-limit" => 5,
            "x-retry-delay"    => 200,
          })
          q = ch.queue("retry-backoff", args: args)
          ch.default_exchange.publish_confirm("msg", q.name)

          delays = [] of Time::Span
          3.times do
            start = Time.instant
            msg = wait_for(timeout: 10.seconds) { q.get(no_ack: false) }
            delays << Time.instant - start
            msg.nack(requeue: true)
          end

          delays[1].should be >= 180.milliseconds
          delays[1].should be < 600.milliseconds
          delays[2].should be >= 380.milliseconds
          delays[2].should be < 1000.milliseconds
        end
      end
    end

    it "should cap delay at x-retry-max-delay" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-delivery-limit"  => 5,
            "x-retry-delay"     => 100,
            "x-retry-max-delay" => 250,
          })
          q = ch.queue("retry-cap", args: args)
          ch.default_exchange.publish_confirm("msg", q.name)

          msg = wait_for { q.get(no_ack: false) }
          msg.nack(requeue: true)
          start = Time.instant
          msg = wait_for(timeout: 5.seconds) { q.get(no_ack: false) }
          delay1 = Time.instant - start
          delay1.should be >= 80.milliseconds
          delay1.should be < 300.milliseconds

          msg.nack(requeue: true)
          start = Time.instant
          msg = wait_for(timeout: 5.seconds) { q.get(no_ack: false) }
          delay2 = Time.instant - start
          delay2.should be >= 180.milliseconds
          delay2.should be < 500.milliseconds

          msg.nack(requeue: true)
          start = Time.instant
          msg = wait_for(timeout: 5.seconds) { q.get(no_ack: false) }
          delay3 = Time.instant - start
          delay3.should be >= 230.milliseconds
          delay3.should be < 600.milliseconds

          msg.ack
        end
      end
    end

    it "should use custom multiplier" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-delivery-limit"         => 5,
            "x-retry-delay"            => 100,
            "x-retry-delay-multiplier" => 3,
          })
          q = ch.queue("retry-multiplier", args: args)
          ch.default_exchange.publish_confirm("msg", q.name)

          msg = wait_for { q.get(no_ack: false) }
          msg.nack(requeue: true)
          start = Time.instant
          msg = wait_for(timeout: 5.seconds) { q.get(no_ack: false) }
          delay1 = Time.instant - start
          delay1.should be >= 80.milliseconds

          msg.nack(requeue: true)
          start = Time.instant
          msg = wait_for(timeout: 5.seconds) { q.get(no_ack: false) }
          delay2 = Time.instant - start
          delay2.should be >= 280.milliseconds

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
            "x-retry-delay"             => 1,
            "x-dead-letter-exchange"    => "",
            "x-dead-letter-routing-key" => "retry-dlq",
          })
          q = ch.queue("retry-exhaust-dlx", args: args)
          ch.default_exchange.publish_confirm("dlx test", q.name)

          3.times do
            msg = wait_for { q.get(no_ack: false) }
            msg.nack(requeue: true)
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
            "x-delivery-limit" => 2,
            "x-retry-delay"    => 1,
          })
          q = ch.queue("retry-exhaust-discard", args: args)
          ch.default_exchange.publish_confirm("discard test", q.name)

          3.times do
            msg = wait_for { q.get(no_ack: false) }
            msg.nack(requeue: true)
          end

          sleep 50.milliseconds
          q.get(no_ack: true).should be_nil
          s.vhosts["/"].queues["retry-exhaust-discard"].message_count.should eq 0
        end
      end
    end

    it "should stop retrying after max delay is reached" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          dlq = ch.queue("retry-max-delay-dlq")
          args = AMQP::Client::Arguments.new({
            "x-retry-delay"             => 1,
            "x-retry-delay-multiplier"  => 2,
            "x-retry-max-delay"         => 3,
            "x-dead-letter-exchange"    => "",
            "x-dead-letter-routing-key" => "retry-max-delay-dlq",
          })
          q = ch.queue("retry-max-delay-stop", args: args)
          ch.default_exchange.publish_confirm("max delay test", q.name)

          # delay=1 (< max), retry
          msg = wait_for { q.get(no_ack: false) }
          msg.nack(requeue: true)
          # delay=2 (< max), retry
          msg = wait_for { q.get(no_ack: false) }
          msg.nack(requeue: true)
          # delay=4 capped to 3 (>= max), last retry
          msg = wait_for { q.get(no_ack: false) }
          msg.nack(requeue: true)
          # previous delay hit max, done — goes to DLX
          msg = wait_for { q.get(no_ack: false) }
          msg.nack(requeue: true)

          dlq_msg = wait_for { dlq.get(no_ack: true) }
          dlq_msg.body_io.to_s.should eq "max delay test"
        end
      end
    end

    it "should discard after max delay when no DLX" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-retry-delay"            => 1,
            "x-retry-delay-multiplier" => 2,
            "x-retry-max-delay"        => 1,
          })
          q = ch.queue("retry-max-delay-discard", args: args)
          ch.default_exchange.publish_confirm("discard test", q.name)

          # delay=1 (>= max), last retry
          msg = wait_for { q.get(no_ack: false) }
          msg.nack(requeue: true)
          # previous delay hit max, done — discard
          msg = wait_for { q.get(no_ack: false) }
          msg.nack(requeue: true)

          sleep 50.milliseconds
          q.get(no_ack: true).should be_nil
        end
      end
    end
  end

  describe "Cleanup" do
    it "should delete retry queue when primary queue is deleted" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-delivery-limit" => 3,
            "x-retry-delay"    => 1000,
          })
          q = ch.queue("retry-cleanup", args: args)
          s.vhosts["/"].queues["amq.retry-retry-cleanup"]?.should_not be_nil
          q.delete
          s.vhosts["/"].queues["amq.retry-retry-cleanup"]?.should be_nil
        end
      end
    end

    it "should not error when primary queue is deleted while messages are in retry queue" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-delivery-limit" => 3,
            "x-retry-delay"    => 60000,
          })
          q = ch.queue("retry-delete-pending", args: args)
          ch.default_exchange.publish_confirm("pending msg", q.name)

          msg = wait_for { q.get(no_ack: false) }
          msg.nack(requeue: true)

          sleep 10.milliseconds
          s.vhosts["/"].queues["amq.retry-retry-delete-pending"].message_count.should eq 1

          q.delete
          s.vhosts["/"].queues["amq.retry-retry-delete-pending"]?.should be_nil
          s.vhosts["/"].queues["retry-delete-pending"]?.should be_nil
        end
      end
    end
  end

  describe "Durability" do
    it "should survive broker restart" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-delivery-limit" => 3,
            "x-retry-delay"    => 100,
          })
          q = ch.queue("retry-durable", durable: true, args: args)
          ch.default_exchange.publish_confirm("persist", q.name)
          msg = wait_for { q.get(no_ack: false) }
          msg.nack(requeue: true)
          sleep 10.milliseconds
          s.vhosts["/"].queues["amq.retry-retry-durable"].message_count.should eq 1
        end

        s.restart

        s.vhosts["/"].queues["amq.retry-retry-durable"]?.should_not be_nil
        with_channel(s) do |ch|
          q = ch.queue("retry-durable", durable: true, args: AMQP::Client::Arguments.new({
            "x-delivery-limit" => 3,
            "x-retry-delay"    => 100,
          }))
          msg = wait_for { q.get(no_ack: true) }
          msg.body_io.to_s.should eq "persist"
        end
      end
    end
  end
end
