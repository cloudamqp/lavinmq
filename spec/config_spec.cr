require "./spec_helper"
require "../src/lavinmq/config"

describe LavinMQ::Config do
  it "should remember the config file path" do
    config_file = File.tempfile do |file|
      file.print <<-CONFIG
        [main]
        log_level = fatal
        data_dir = /tmp/lavinmq-spec
        [mgmt]
        [amqp]
      CONFIG
    end
    config = LavinMQ::Config.new
    config.config_file = config_file.path
    config.parse
    config.config_file.should eq config_file.path
    config.data_dir.should eq "/tmp/lavinmq-spec"
    config.log_level.to_s.should eq "Fatal"
  end

  it "raises on non-hashed default_password" do
    config = LavinMQ::Config.new
    config.default_password = "abc"
    expect_raises(ArgumentError) do
      config.verify_default_password
    end
  end

  it "handles hashed default_password" do
    config = LavinMQ::Config.new
    config.default_password = "+pHuxkR9fCyrrwXjOD4BP4XbzO3l8LJr8YkThMgJ0yVHFRE+"
    config.verify_default_password
  end
end
