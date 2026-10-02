require "../spec_helper"
require "../../src/lavinmq/clustering/status_server"

private def with_status_server(&)
  dir = File.tempname("lavinmq", "status-spec")
  Dir.mkdir_p dir
  path = File.join(dir, "status.sock")
  status = LavinMQ::Clustering::LeaderStatus.new
  server = LavinMQ::Clustering::StatusServer.new(status, path)
  server.bind
  spawn server.listen
  yield status, path, server
ensure
  server.try &.close
  FileUtils.rm_rf dir if dir
end

private def read_line(io : IO) : String?
  io.read_timeout = 5.seconds
  io.gets
end

describe LavinMQ::Clustering::LeaderStatus do
  it "only notifies subscribers on a real change" do
    status = LavinMQ::Clustering::LeaderStatus.new
    status.raft_state(true, 2i64, "tcp://a:5679")
    ch = status.subscribe
    ch.receive
    status.raft_state(true, 2i64, "tcp://a:5679")
    select
    when ch.receive
      fail "expected no snapshot for an unchanged state"
    else
    end
    status.ready = true
    ch.receive.ready.should be_true
  end

  it "keeps only the latest snapshot for a subscriber that doesn't read" do
    status = LavinMQ::Clustering::LeaderStatus.new
    ch = status.subscribe
    10.times { |i| status.raft_state(false, i.to_i64 + 1, nil) }
    ch.receive.term.should eq 10
    select
    when ch.receive
      fail "expected no more snapshots"
    else
    end
  end

  it "formats snapshots as key=value" do
    s = LavinMQ::Clustering::LeaderStatus::Snapshot.new(true, true, 4i64, "tcp://a:5679")
    s.to_s.should eq "ready=1 leader=1 term=4 leader_uri=tcp://a:5679"
    s.copy_with(ready: false, leader: false, leader_uri: nil).to_s.should eq "ready=0 leader=0 term=4 leader_uri="
  end
end

describe LavinMQ::Clustering::StatusServer do
  it "streams the current state and every change" do
    with_status_server do |status, path|
      status.raft_state(false, 3i64, "tcp://a:5679")
      UNIXSocket.open(path) do |io|
        read_line(io).should eq "ready=0 leader=0 term=3 leader_uri=tcp://a:5679"
        status.raft_state(true, 4i64, "tcp://b:5679")
        read_line(io).should eq "ready=0 leader=1 term=4 leader_uri=tcp://b:5679"
        status.ready = true
        read_line(io).should eq "ready=1 leader=1 term=4 leader_uri=tcp://b:5679"
      end
    end
  end

  it "unsubscribes clients that disconnect" do
    with_status_server do |status, path|
      UNIXSocket.open(path) { |io| read_line(io).should_not be_nil }
      wait_for { status.subscriber_count == 0 }
    end
  end

  it "sends the last state and then EOF when closed" do
    with_status_server do |status, path, server|
      status.ready = true
      UNIXSocket.open(path) do |io|
        read_line(io).not_nil!.should start_with "ready=1"
        status.ready = false
        server.close
        read_line(io).not_nil!.should start_with "ready=0"
        read_line(io).should be_nil
      end
    end
  end
end
