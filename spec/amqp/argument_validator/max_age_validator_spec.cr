require "../../spec_helper"

describe LavinMQ::AMQP::ArgumentValidator::MaxAgeValidator do
  describe "#validate!" do
    it "accepts valid year format" do
      validator = LavinMQ::AMQP::ArgumentValidator::MaxAgeValidator.new
      validator.validate!("x-max-age", "1Y").should be_nil
    end

    it "accepts valid month format" do
      validator = LavinMQ::AMQP::ArgumentValidator::MaxAgeValidator.new
      validator.validate!("x-max-age", "6M").should be_nil
    end

    it "accepts valid day format" do
      validator = LavinMQ::AMQP::ArgumentValidator::MaxAgeValidator.new
      validator.validate!("x-max-age", "30D").should be_nil
    end

    it "accepts valid hour format" do
      validator = LavinMQ::AMQP::ArgumentValidator::MaxAgeValidator.new
      validator.validate!("x-max-age", "24h").should be_nil
    end

    it "accepts valid minute format" do
      validator = LavinMQ::AMQP::ArgumentValidator::MaxAgeValidator.new
      validator.validate!("x-max-age", "60m").should be_nil
    end

    it "accepts valid second format" do
      validator = LavinMQ::AMQP::ArgumentValidator::MaxAgeValidator.new
      validator.validate!("x-max-age", "3600s").should be_nil
    end

    it "accepts large numbers" do
      validator = LavinMQ::AMQP::ArgumentValidator::MaxAgeValidator.new
      validator.validate!("x-max-age", "999999Y").should be_nil
    end

    it "rejects non-string values" do
      validator = LavinMQ::AMQP::ArgumentValidator::MaxAgeValidator.new
      expect_raises(LavinMQ::Error::PreconditionFailed, "x-max-age must be a string") do
        validator.validate!("x-max-age", 100_i64)
      end
    end

    it "rejects missing unit" do
      validator = LavinMQ::AMQP::ArgumentValidator::MaxAgeValidator.new
      expect_raises(LavinMQ::Error::PreconditionFailed, "x-max-age format invalid") do
        validator.validate!("x-max-age", "100")
      end
    end

    it "rejects invalid unit" do
      validator = LavinMQ::AMQP::ArgumentValidator::MaxAgeValidator.new
      expect_raises(LavinMQ::Error::PreconditionFailed, "x-max-age format invalid") do
        validator.validate!("x-max-age", "100x")
      end
    end

    it "rejects missing number" do
      validator = LavinMQ::AMQP::ArgumentValidator::MaxAgeValidator.new
      expect_raises(LavinMQ::Error::PreconditionFailed, "x-max-age format invalid") do
        validator.validate!("x-max-age", "D")
      end
    end

    it "rejects space before unit" do
      validator = LavinMQ::AMQP::ArgumentValidator::MaxAgeValidator.new
      expect_raises(LavinMQ::Error::PreconditionFailed, "x-max-age format invalid") do
        validator.validate!("x-max-age", "100 D")
      end
    end

    it "rejects multiple units" do
      validator = LavinMQ::AMQP::ArgumentValidator::MaxAgeValidator.new
      expect_raises(LavinMQ::Error::PreconditionFailed, "x-max-age format invalid") do
        validator.validate!("x-max-age", "1D2h")
      end
    end

    it "rejects empty string" do
      validator = LavinMQ::AMQP::ArgumentValidator::MaxAgeValidator.new
      expect_raises(LavinMQ::Error::PreconditionFailed, "x-max-age format invalid") do
        validator.validate!("x-max-age", "")
      end
    end

    it "rejects negative numbers" do
      validator = LavinMQ::AMQP::ArgumentValidator::MaxAgeValidator.new
      expect_raises(LavinMQ::Error::PreconditionFailed, "x-max-age format invalid") do
        validator.validate!("x-max-age", "-5D")
      end
    end

    it "rejects decimal numbers" do
      validator = LavinMQ::AMQP::ArgumentValidator::MaxAgeValidator.new
      expect_raises(LavinMQ::Error::PreconditionFailed, "x-max-age format invalid") do
        validator.validate!("x-max-age", "1.5D")
      end
    end
  end
end
