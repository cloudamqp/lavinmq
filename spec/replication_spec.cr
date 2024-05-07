require "./spec_helper"
require "../src/lavinmq/replication/client"

describe LavinMQ::Replication::Client do
  data_dir = "/tmp/lavinmq-follower"

  before_each do
    FileUtils.rm_rf data_dir
    Dir.mkdir_p data_dir
    File.write File.join(data_dir, ".replication_secret"), Server.@replicator.@password, 0o400
  end

  after_each do
    FileUtils.rm_rf data_dir
  end

  it "can synchronize" do
    with_channel do |ch|
      q = ch.queue("repli")
      q.publish_confirm "hello world"
    end
    repli = LavinMQ::Replication::Client.new(data_dir)
    repli.sync("127.0.0.1", LavinMQ::Config.instance.replication_port)
    repli.close

    server = LavinMQ::Server.new(data_dir)
    begin
      q = server.vhosts["/"].queues["repli"].as(LavinMQ::DurableQueue)
      q.basic_get(true) do |env|
        String.new(env.message.body).to_s.should eq "hello world"
      end.should be_true
    ensure
      server.close
    end
  end

  it "can stream changes" do
    done = Channel(Nil).new
    repli = LavinMQ::Replication::Client.new(data_dir)
    spawn do
      repli.follow("127.0.0.1", LavinMQ::Config.instance.replication_port)
      done.send nil
    end
    with_channel do |ch|
      q = ch.queue("repli")
      q.publish_confirm "hello world"
    end
    {% if flag?(:freebsd) %}
      sleep 1
    {% else %}
      sleep 0.1
    {% end %}
    repli.close
    done.receive

    server = LavinMQ::Server.new(data_dir)
    begin
      q = server.vhosts["/"].queues["repli"].as(LavinMQ::DurableQueue)
      q.message_count.should eq 1
      q.basic_get(true) do |env|
        String.new(env.message.body).to_s.should eq "hello world"
      end.should be_true
    ensure
      server.close
    end
  end
end

describe LavinMQ::Replication::Server do
  data_dir = "/tmp/lavinmq-follower"

  before_each do
    FileUtils.rm_rf data_dir
    Dir.mkdir_p data_dir
    File.write File.join(data_dir, ".replication_secret"), Server.@replicator.@password, 0o400
    Server.vhosts["/"].declare_queue("repli", true, false)
    LavinMQ::Config.instance.min_followers = 1
  end

  after_each do
    FileUtils.rm_rf data_dir
    LavinMQ::Config.instance.min_followers = 0
  end


  it "should shut down gracefully" do
    repli = LavinMQ::Replication::Client.new(data_dir)
    3.times do
      spawn do
        repli.follow("127.0.0.1", LavinMQ::Config.instance.replication_port)
      end
    end

    # repli.closing

  end

  it "should publish when min_followers is fulfilled" do
    q = Server.vhosts["/"].queues["repli"].as(LavinMQ::Queue)
    repli = LavinMQ::Replication::Client.new(data_dir)
    spawn do
      repli.follow("127.0.0.1", LavinMQ::Config.instance.replication_port)
    end
    with_channel do |ch|
      ch.basic_publish "hello world", "", "repli"
    end
    q.basic_get(true) { }.should be_true
    repli.close
  end

  it "should not publish when min_followers is not fulfilled" do
    done = Channel(Nil).new
    client : AMQP::Client::Connection? = nil
    spawn do
      with_channel do |ch, conn|
        client = conn
        q = ch.queue("repli")
        q.publish_confirm "hello world"
        done.send nil
      end
    end
    select
    when done.receive
      fail "Should not receive message"
    when timeout(0.1.seconds)
      client.try &.close(no_wait: true)
      Server.close
    end
  end

  # it "should publish when max_lag is not reached" do
  #   LavinMQ::Config.instance.max_lag = 10000
  #   q = Server.vhosts["/"].queues["repli"].as(LavinMQ::Queue)
  #   repli = LavinMQ::Replication::Client.new(data_dir)
  #   spawn do
  #     repli.follow("127.0.0.1", LavinMQ::Config.instance.replication_port)
  #   end
  #   with_channel do |ch|
  #     ch.basic_publish "hello world", "", "repli"
  #   end
  #   q.basic_get(true) { }.should be_true
  #   repli.close
  # end

  # it "should not publish when max_lag is reached" do
  # end
end
