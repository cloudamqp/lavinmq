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
        [auth]
      CONFIG
    end
    config = LavinMQ::Config.new
    config.config_file = config_file.path
    config.parse
    config.config_file.should eq config_file.path
    config.data_dir.should eq "/tmp/lavinmq-spec"
    config.log_level.to_s.should eq "Fatal"
  end
end
