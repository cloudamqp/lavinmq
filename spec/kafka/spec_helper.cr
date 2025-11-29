require "crafka"
require "../spec_helper"

module KafkaHelpers
  def with_kafka_server(&)
    kafka_server = TCPServer.new("localhost", 0)
    amqp_server = TCPServer.new("localhost", 0)
    s = LavinMQ::Server.new(LavinMQ::Config.instance, nil)
    begin
      spawn(name: "amqp tcp listen") { s.listen(amqp_server, LavinMQ::Server::Protocol::AMQP) }
      spawn(name: "kafka tcp listen") { s.listen(kafka_server, LavinMQ::Server::Protocol::Kafka) }
      Fiber.yield
      yield s, kafka_server.local_address.port
    ensure
      s.close
      FileUtils.rm_rf(LavinMQ::Config.instance.data_dir)
    end
  end

  def kafka_bootstrap_servers(port : Int32) : String
    "localhost:#{port}"
  end
end
