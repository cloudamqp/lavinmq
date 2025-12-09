require "../../spec_helper"

describe LavinMQ::AMQP::ArgumentValidator::DeadLetteringValidator do
  describe "#validate!" do
    it "allows nil values when dlx is set" do
      arguments = LavinMQ::AMQP::Table.new({"x-dead-letter-exchange" => "dlx"})
      validator = LavinMQ::AMQP::ArgumentValidator::DeadLetteringValidator.new(arguments)
      validator.validate!("x-dead-letter-routing-key", nil).should be_nil
    end

    it "accepts valid string routing key when dlx is set" do
      arguments = LavinMQ::AMQP::Table.new({"x-dead-letter-exchange" => "dlx"})
      validator = LavinMQ::AMQP::ArgumentValidator::DeadLetteringValidator.new(arguments)
      validator.validate!("x-dead-letter-routing-key", "my-routing-key").should be_nil
    end

    it "accepts empty string routing key when dlx is set" do
      arguments = LavinMQ::AMQP::Table.new({"x-dead-letter-exchange" => "dlx"})
      validator = LavinMQ::AMQP::ArgumentValidator::DeadLetteringValidator.new(arguments)
      validator.validate!("x-dead-letter-routing-key", "").should be_nil
    end

    it "rejects routing key when dlx is not set" do
      arguments = LavinMQ::AMQP::Table.new
      validator = LavinMQ::AMQP::ArgumentValidator::DeadLetteringValidator.new(arguments)
      expect_raises(LavinMQ::Error::PreconditionFailed, "x-dead-letter-exchange required if x-dead-letter-routing-key is defined") do
        validator.validate!("x-dead-letter-routing-key", "my-routing-key")
      end
    end

    it "rejects non-string routing key" do
      arguments = LavinMQ::AMQP::Table.new({"x-dead-letter-exchange" => "dlx"})
      validator = LavinMQ::AMQP::ArgumentValidator::DeadLetteringValidator.new(arguments)
      expect_raises(LavinMQ::Error::PreconditionFailed, "x-dead-letter-routing-key header not a string") do
        validator.validate!("x-dead-letter-routing-key", 123_i64)
      end
    end

    it "rejects boolean routing key" do
      arguments = LavinMQ::AMQP::Table.new({"x-dead-letter-exchange" => "dlx"})
      validator = LavinMQ::AMQP::ArgumentValidator::DeadLetteringValidator.new(arguments)
      expect_raises(LavinMQ::Error::PreconditionFailed, "x-dead-letter-routing-key header not a string") do
        validator.validate!("x-dead-letter-routing-key", true)
      end
    end

    it "rejects integer routing key when dlx is not set" do
      arguments = LavinMQ::AMQP::Table.new
      validator = LavinMQ::AMQP::ArgumentValidator::DeadLetteringValidator.new(arguments)
      expect_raises(LavinMQ::Error::PreconditionFailed, "x-dead-letter-routing-key header not a string") do
        validator.validate!("x-dead-letter-routing-key", 456_i64)
      end
    end

    it "accepts routing key when dlx is empty string" do
      arguments = LavinMQ::AMQP::Table.new({"x-dead-letter-exchange" => ""})
      validator = LavinMQ::AMQP::ArgumentValidator::DeadLetteringValidator.new(arguments)
      validator.validate!("x-dead-letter-routing-key", "my-key").should be_nil
    end

    it "handles non-string dlx value gracefully" do
      arguments = LavinMQ::AMQP::Table.new({"x-dead-letter-exchange" => 123_i64})
      validator = LavinMQ::AMQP::ArgumentValidator::DeadLetteringValidator.new(arguments)
      expect_raises(LavinMQ::Error::PreconditionFailed, "x-dead-letter-exchange required if x-dead-letter-routing-key is defined") do
        validator.validate!("x-dead-letter-routing-key", "my-key")
      end
    end
  end
end
