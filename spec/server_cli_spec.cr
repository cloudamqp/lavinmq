require "./spec_helper"
require "../src/avalanchemq/server_cli"

describe "AvalancheMQ::ServerCLI" do
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
    config = AvalancheMQ::Config.instance
    AvalancheMQ::ServerCLI.new(config, config_file.path).parse

    config.config_file.should eq(config_file.path)
  end
end
