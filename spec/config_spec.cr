require "./spec_helper"
require "../src/lavinmq/config"

describe LavinMQ::Config do
  it "should remember the config file path" do
    config_file = File.tempfile do |file|
      file.print <<-CONFIG
        [main]
        log_level = info
        data_dir = /tmp/lavinmq-spec
        [mgmt]
        [amqp]
      CONFIG
    end
    config = LavinMQ::Config.instance
    config.config_file = config_file.path
    config.parse
    config.config_file.should eq config_file.path
  end
end
