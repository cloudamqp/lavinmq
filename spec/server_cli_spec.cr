require "./spec_helper"
require "../src/lavinmq/server_cli"

describe "LavinMQ::ServerCLI" do
  it "should remember the config file path" do
    config_file = File.tempfile do |file|
      file.print <<-CONFIG
        [main]
        log_level = info
        data_dir = /tmp/spec
        [mgmt]
        [amqp]
      CONFIG
    end
    config = LavinMQ::Config.instance
    config.config_file = config_file.path
    LavinMQ::ServerCLI.new(config).parse

    config.config_file.should eq(config_file.path)
  end
end
