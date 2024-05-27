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
