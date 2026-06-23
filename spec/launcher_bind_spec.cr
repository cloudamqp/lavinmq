require "./spec_helper"
require "../src/lavinmq/launcher"

class LavinMQ::Launcher
  def start_for_spec
    start
  end
end

describe LavinMQ::Launcher do
  it "aborts startup when the AMQP listener can not bind" do
    blocker = TCPServer.new("127.0.0.1", 0)
    with_datadir do |data_dir|
      launcher : LavinMQ::Launcher? = nil
      config = LavinMQ::Config.new
      config.data_dir = data_dir
      config.data_dir_lock = false
      config.amqp_bind = "127.0.0.1"
      config.amqp_port = blocker.local_address.port
      config.amqps_port = -1
      config.http_port = -1
      config.https_port = -1
      config.mqtt_port = -1
      config.mqtts_port = -1
      config.metrics_http_port = -1
      config.control_unix_path = File.join(data_dir, "control.sock")
      launcher = LavinMQ::Launcher.new(config)

      expect_raises(SpecExit, /Exiting with code 1/) do
        launcher.not_nil!.start_for_spec
      end
    ensure
      launcher.try &.stop
    end
  ensure
    blocker.try &.close
  end
end
