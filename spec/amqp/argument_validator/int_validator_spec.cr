require "../../spec_helper"

describe LavinMQ::AMQP::ArgumentValidator::IntValidator do
  describe "#validate!" do
    it "allows nil values" do
      validator = LavinMQ::AMQP::ArgumentValidator::IntValidator.new
      validator.validate!("x-test", nil).should be_nil
    end

    it "accepts valid integer" do
      validator = LavinMQ::AMQP::ArgumentValidator::IntValidator.new
      validator.validate!("x-test", 100_i64).should be_nil
    end

    it "rejects non-integer values" do
      validator = LavinMQ::AMQP::ArgumentValidator::IntValidator.new
      expect_raises(LavinMQ::Error::PreconditionFailed, "x-test header not an integer") do
        validator.validate!("x-test", "not-an-int")
      end
    end

    it "rejects boolean values" do
      validator = LavinMQ::AMQP::ArgumentValidator::IntValidator.new
      expect_raises(LavinMQ::Error::PreconditionFailed, "x-test header not an integer") do
        validator.validate!("x-test", true)
      end
    end

    it "accepts value above minimum" do
      validator = LavinMQ::AMQP::ArgumentValidator::IntValidator.new(min_value: 10)
      validator.validate!("x-test", 15_i64).should be_nil
    end

    it "accepts value equal to minimum" do
      validator = LavinMQ::AMQP::ArgumentValidator::IntValidator.new(min_value: 10)
      validator.validate!("x-test", 10_i64).should be_nil
    end

    it "rejects value below minimum" do
      validator = LavinMQ::AMQP::ArgumentValidator::IntValidator.new(min_value: 10)
      expect_raises(LavinMQ::Error::PreconditionFailed, "x-test header less than minimum value 10") do
        validator.validate!("x-test", 5_i64)
      end
    end

    it "accepts value below maximum" do
      validator = LavinMQ::AMQP::ArgumentValidator::IntValidator.new(max_value: 100)
      validator.validate!("x-test", 50_i64).should be_nil
    end

    it "accepts value equal to maximum" do
      validator = LavinMQ::AMQP::ArgumentValidator::IntValidator.new(max_value: 100)
      validator.validate!("x-test", 100_i64).should be_nil
    end

    it "rejects value above maximum" do
      validator = LavinMQ::AMQP::ArgumentValidator::IntValidator.new(max_value: 100)
      expect_raises(LavinMQ::Error::PreconditionFailed, "x-test header greater than maximum value 100") do
        validator.validate!("x-test", 150_i64)
      end
    end

    it "accepts value within min and max range" do
      validator = LavinMQ::AMQP::ArgumentValidator::IntValidator.new(min_value: 10, max_value: 100)
      validator.validate!("x-test", 50_i64).should be_nil
    end

    it "rejects value below min when both min and max set" do
      validator = LavinMQ::AMQP::ArgumentValidator::IntValidator.new(min_value: 10, max_value: 100)
      expect_raises(LavinMQ::Error::PreconditionFailed, "x-test header less than minimum value 10") do
        validator.validate!("x-test", 5_i64)
      end
    end

    it "rejects value above max when both min and max set" do
      validator = LavinMQ::AMQP::ArgumentValidator::IntValidator.new(min_value: 10, max_value: 100)
      expect_raises(LavinMQ::Error::PreconditionFailed, "x-test header greater than maximum value 100") do
        validator.validate!("x-test", 150_i64)
      end
    end
  end
end
