require "./spec_helper"
require "../src/lavinmq/clustering/client"

describe LavinMQ::Clustering::Client do
  data_dir = "/tmp/lavinmq-follower"

  before_each do
    FileUtils.rm_rf data_dir
    Dir.mkdir_p data_dir
  end

  after_each do
    FileUtils.rm_rf data_dir
  end

  pending "can synchronize" do
    with_channel do |ch|
      q = ch.queue("repli")
      q.publish_confirm "hello world"
    end
    repli = LavinMQ::Clustering::Client.new(data_dir, 1, Server.@replicator.password, proxy: false)
    repli.sync("127.1", LavinMQ::Config.instance.clustering_port)
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

  pending "can stream changes" do
    repli = LavinMQ::Clustering::Client.new(data_dir, 1, Server.@replicator.password, proxy: false)
    done = Channel(Nil).new
    spawn do
      repli.follow("127.0.0.1", LavinMQ::Config.instance.clustering_port)
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
