require "../spec_helper"
require "../../src/lavinmq/clustering/server"

# Every file a leader keeps under its data dir must be known to the
# replicator, or a follower promoted after a failover would lack it (the
# full sync even deletes what the leader never listed). Files that are local
# to a node by design are listed below; anything else is a bug.
private LOCAL_ONLY = LavinMQ::Clustering::NODE_LOCAL_FILES + {"checksums.sha1", "checksums.sha1.tmp"}

private def data_dir_files(data_dir : String) : Array(String)
  files = Array(String).new
  Dir.glob(File.join(data_dir, "**", "*"), match: File::MatchOptions::DotFiles) do |path|
    next unless File.file?(path)
    relative = path.lchop(data_dir).lchop('/')
    next if LOCAL_ONLY.includes?(relative)
    next if relative.ends_with?(".tmp")
    # Non-durable data is deliberately not replicated
    next if relative.includes?("/transient/")
    next if relative.starts_with?("backups/")
    files << relative
  end
  files.sort!
end

describe "replicated files" do
  it "registers every file in the data dir with the replicator" do
    server = LavinMQ::Clustering::Server.new(LavinMQ::Config.instance, NullCoordinator.new, 0)
    with_amqp_server(replicator: server) do |s|
      s.vhosts.create("reg")
      s.users.create("reg-user", "pw")
      s.users.add_permission("reg-user", "reg", /.*/, /.*/, /.*/)
      vhost = s.vhosts["reg"]
      vhost.add_policy("reg-policy", "^reg", "queues", {"max-length" => JSON::Any.new(10i64)}, 0i8)
      vhost.add_parameter(LavinMQ::Parameter.new("shovel", "reg-shovel", JSON::Any.new({
        "src-uri"    => JSON::Any.new("amqp://127.0.0.1:1"),
        "src-queue"  => JSON::Any.new("reg-src"),
        "dest-uri"   => JSON::Any.new("amqp://127.0.0.1:1"),
        "dest-queue" => JSON::Any.new("reg-dest"),
      })))
      with_channel(s, vhost: "reg") do |ch|
        ch.exchange("reg-exchange", "topic", durable: true)
        q = ch.queue("reg-queue", durable: true)
        q.bind("reg-exchange", "reg.#")
        q.publish_confirm "a message", props: AMQP::Client::Properties.new(delivery_mode: 2_u8)
        ch.queue("reg-stream", durable: true, args: AMQP::Client::Arguments.new({"x-queue-type" => "stream"}))
          .publish_confirm "a record"
        ch.queue("reg-prio", durable: true, args: AMQP::Client::Arguments.new({"x-max-priority" => 3}))
          .publish_confirm "prioritised"
      end
      vhost.queue("reg-queue").as(LavinMQ::AMQP::DurableQueue).pause!
      vhost.shovels.each_value &.pause
      wait_for do
        paused = true
        vhost.shovels.each_value { |shovel| paused &&= shovel.paused? }
        paused
      end

      files = data_dir_files(LavinMQ::Config.instance.data_dir)
      files.size.should be > 10 # the walk found the vhost, its queues and their segments
      files.should contain "#{vhost.dir}/.vhost"
      files.should contain "#{vhost.dir}/#{Digest::SHA1.hexdigest("reg-queue")}/paused"
      unregistered = files.reject do |relative|
        server.registered?(File.join(LavinMQ::Config.instance.data_dir, relative))
      end
      unregistered.should be_empty
    end
  ensure
    server.try &.close
  end
end
