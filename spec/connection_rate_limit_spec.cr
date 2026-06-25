require "./spec_helper"

describe LavinMQ::ConnectionRateLimiter do
  describe "global rate limit" do
    it "allows connections when disabled (limit=0)" do
      config = LavinMQ::Config.new
      config.connection_rate_limit = 0
      limiter = LavinMQ::ConnectionRateLimiter.new(config)
      100.times { limiter.allow?("127.0.0.1").should be_true }
    end

    it "allows connections up to the limit" do
      config = LavinMQ::Config.new
      config.connection_rate_limit = 5
      limiter = LavinMQ::ConnectionRateLimiter.new(config)
      5.times { |i| limiter.allow?("127.0.0.1").should(be_true, "failed on attempt #{i}") }
      limiter.allow?("127.0.0.1").should be_false
    end

    it "replenishes tokens over time" do
      config = LavinMQ::Config.new
      config.connection_rate_limit = 10
      limiter = LavinMQ::ConnectionRateLimiter.new(config)
      10.times { limiter.allow?("127.0.0.1") }
      limiter.allow?("127.0.0.1").should be_false
      sleep 0.2.seconds
      limiter.allow?("127.0.0.1").should be_true
    end
  end

  describe "per-IP rate limit" do
    it "allows connections when disabled (limit=0)" do
      config = LavinMQ::Config.new
      config.connection_rate_limit_per_ip = 0
      limiter = LavinMQ::ConnectionRateLimiter.new(config)
      100.times { limiter.allow?("10.0.0.1").should be_true }
    end

    it "limits each IP independently" do
      config = LavinMQ::Config.new
      config.connection_rate_limit_per_ip = 2
      limiter = LavinMQ::ConnectionRateLimiter.new(config)
      2.times { limiter.allow?("10.0.0.1").should be_true }
      limiter.allow?("10.0.0.1").should be_false
      2.times { limiter.allow?("10.0.0.2").should be_true }
      limiter.allow?("10.0.0.2").should be_false
    end

    it "replenishes per-IP tokens over time" do
      config = LavinMQ::Config.new
      config.connection_rate_limit_per_ip = 10
      limiter = LavinMQ::ConnectionRateLimiter.new(config)
      10.times { limiter.allow?("10.0.0.1") }
      limiter.allow?("10.0.0.1").should be_false
      sleep 0.2.seconds
      limiter.allow?("10.0.0.1").should be_true
    end
  end

  describe "combined global and per-IP limits" do
    it "per-IP limit stops connections before global limit" do
      config = LavinMQ::Config.new
      config.connection_rate_limit = 10
      config.connection_rate_limit_per_ip = 2
      limiter = LavinMQ::ConnectionRateLimiter.new(config)
      2.times { limiter.allow?("10.0.0.1").should be_true }
      # Per-IP limit reached, even though global has tokens
      limiter.allow?("10.0.0.1").should be_false
      # Different IP still works
      2.times { limiter.allow?("10.0.0.2").should be_true }
      limiter.allow?("10.0.0.2").should be_false
    end

    it "global limit stops connections even if per-IP has tokens" do
      config = LavinMQ::Config.new
      config.connection_rate_limit = 3
      config.connection_rate_limit_per_ip = 5
      limiter = LavinMQ::ConnectionRateLimiter.new(config)
      3.times { limiter.allow?("10.0.0.1").should be_true }
      # Global limit reached
      limiter.allow?("10.0.0.1").should be_false
      # Different IP also blocked by global limit
      limiter.allow?("10.0.0.2").should be_false
    end
  end
end

describe "Server connection rate limiting" do
  it "rejects connections exceeding the global rate limit" do
    config = LavinMQ::Config.new
    config.connection_rate_limit = 2
    with_amqp_server(config: config) do |s|
      conn1 = AMQP::Client.new(port: amqp_port(s)).connect
      conn2 = AMQP::Client.new(port: amqp_port(s)).connect
      # Third connection should fail - server closes socket before handshake
      expect_raises(Exception) do
        AMQP::Client.new(port: amqp_port(s)).connect
      end
      conn1.close
      conn2.close
    end
  end

  it "allows unlimited connections when rate limit is 0" do
    config = LavinMQ::Config.new
    config.connection_rate_limit = 0
    with_amqp_server(config: config) do |s|
      conns = Array(AMQP::Client::Connection).new
      5.times do
        conns << AMQP::Client.new(port: amqp_port(s)).connect
      end
      conns.size.should eq 5
      conns.each &.close
    end
  end

  it "rejects connections exceeding the per-IP rate limit" do
    config = LavinMQ::Config.new
    config.connection_rate_limit_per_ip = 2
    with_amqp_server(config: config) do |s|
      conn1 = AMQP::Client.new(port: amqp_port(s)).connect
      conn2 = AMQP::Client.new(port: amqp_port(s)).connect
      expect_raises(Exception) do
        AMQP::Client.new(port: amqp_port(s)).connect
      end
      conn1.close
      conn2.close
    end
  end
end
