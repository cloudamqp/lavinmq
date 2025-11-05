require "../../spec_helper"

describe LavinMQ::AMQP::ArgumentValidator::BoolValidator do
  describe "#validate!" do
    it "allows nil values" do
      validator = LavinMQ::AMQP::ArgumentValidator::BoolValidator.new
      validator.validate!("x-test", nil).should be_nil
    end

    it "accepts true" do
      validator = LavinMQ::AMQP::ArgumentValidator::BoolValidator.new
      validator.validate!("x-test", true).should be_nil
    end

    it "accepts false" do
      validator = LavinMQ::AMQP::ArgumentValidator::BoolValidator.new
      validator.validate!("x-test", false).should be_nil
    end

    it "rejects string values" do
      validator = LavinMQ::AMQP::ArgumentValidator::BoolValidator.new
      expect_raises(LavinMQ::Error::PreconditionFailed, "x-test header not a boolean") do
        validator.validate!("x-test", "true")
      end
    end

    it "rejects integer values" do
      validator = LavinMQ::AMQP::ArgumentValidator::BoolValidator.new
      expect_raises(LavinMQ::Error::PreconditionFailed, "x-test header not a boolean") do
        validator.validate!("x-test", 1_i64)
      end
    end

    it "rejects integer zero" do
      validator = LavinMQ::AMQP::ArgumentValidator::BoolValidator.new
      expect_raises(LavinMQ::Error::PreconditionFailed, "x-test header not a boolean") do
        validator.validate!("x-test", 0_i64)
      end
    end
  end
end
