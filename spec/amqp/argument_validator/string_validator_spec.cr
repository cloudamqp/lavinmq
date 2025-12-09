require "../../spec_helper"

describe LavinMQ::AMQP::ArgumentValidator::StringValidator do
  describe "#validate!" do
    it "allows nil values" do
      validator = LavinMQ::AMQP::ArgumentValidator::StringValidator.new
      validator.validate!("x-test", nil).should be_nil
    end

    it "accepts valid string" do
      validator = LavinMQ::AMQP::ArgumentValidator::StringValidator.new
      validator.validate!("x-test", "valid-string").should be_nil
    end

    it "accepts empty string" do
      validator = LavinMQ::AMQP::ArgumentValidator::StringValidator.new
      validator.validate!("x-test", "").should be_nil
    end

    it "rejects integer values" do
      validator = LavinMQ::AMQP::ArgumentValidator::StringValidator.new
      expect_raises(LavinMQ::Error::PreconditionFailed, "x-test header not a string") do
        validator.validate!("x-test", 123_i64)
      end
    end

    it "rejects boolean values" do
      validator = LavinMQ::AMQP::ArgumentValidator::StringValidator.new
      expect_raises(LavinMQ::Error::PreconditionFailed, "x-test header not a string") do
        validator.validate!("x-test", true)
      end
    end

    it "rejects array values" do
      validator = LavinMQ::AMQP::ArgumentValidator::StringValidator.new
      expect_raises(LavinMQ::Error::PreconditionFailed, "x-test header not a string") do
        validator.validate!("x-test", [1, 2, 3])
      end
    end
  end
end
