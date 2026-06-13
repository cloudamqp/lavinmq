require "../spec_helper"
require "../../src/lavinmq/launcher"
require "../../src/lavinmq/clustering/client"
require "../../src/lavinmq/clustering/server"

# Investigation: does a follower that joins (full_sync + mark_synced) while the
# leader is actively publishing end up consistent with the leader, or does the
# write_to_disk gap (local write at message_store.cr:339 before the replicate
# at :340) cause a message to be delivered both in the full-sync file copy AND
# again incrementally, duplicating it on the follower?
describe "clustering join-during-publish race", tags: "etcd" do
  add_etcd_around_each

  it "follower joining during active publishing has the same message count as the leader" do
    follower_dir = File.tempname
    Dir.mkdir follower_dir
    config = LavinMQ::Config.instance # leader uses the example's data dir
    replicator = LavinMQ::Clustering::Server.new(config, NullCoordinator.new, 0)
    tcp_server = TCPServer.new("localhost", 0)
    port = tcp_server.local_address.port
    spawn(replicator.listen(tcp_server), name: "repli server spec")

    leader_hashes = Hash(String, String).new
    leader_sizes = Hash(String, Int64).new
    with_amqp_server(replicator: replicator) do |s|
      # Pre-fill so there are several segments and an actively-written wfile
      # whose hash will mismatch during the joiner's full sync.
      with_channel(s) do |ch|
        q = ch.queue("race", durable: true)
        300.times { q.publish("x" * 2000, props: AMQP::Client::Properties.new(delivery_mode: 2_u8)) }
      end

      # Keep publishing on several concurrent channels so at any instant a
      # write_to_disk is likely parked between its local write (:339) and the
      # replicate (:340) while the joiner holds the lock during full_sync.
      publishing = true
      publishers = WaitGroup.new
      8.times do
        publishers.spawn do
          with_channel(s) do |ch|
            q = ch.queue("race", durable: true)
            while publishing
              q.publish("y" * 1000, props: AMQP::Client::Properties.new(delivery_mode: 2_u8))
            end
          end
        end
      end

      # Follower joins WHILE publishing is in flight.
      follower_config = config.dup.tap &.data_dir = follower_dir
      repli = LavinMQ::Clustering::Client.new(follower_config, 1, replicator.password, proxy: false)
      follower_done = WaitGroup.new(1)
      spawn(name: "follower spec") do
        repli.follow("localhost", port)
        follower_done.done
      end

      sleep 2.seconds # full-sync and stream while publishing continues
      publishing = false
      publishers.wait

      wait_for { replicator.followers.first?.try { |f| f.synced? && f.lag_in_bytes == 0 } }
      sleep 0.2.seconds

      # Oracle: hash every leader file using the leader's own logical-size
      # hashing. A correct follower's on-disk files are byte-identical to this.
      # A full-sync/incremental duplicate makes the follower's file diverge.
      replicator.files_with_hash do |path, hash|
        leader_hashes[path] = hash.hexstring
        leader_sizes[path] = replicator.with_file(path, nil) { |_f, sz| sz }
      end
      repli.close
      follower_done.wait
    end
    replicator.close

    leader_hashes.each do |path, lhash|
      fpath = File.join(follower_dir, path)
      File.exists?(fpath).should be_true, "follower missing #{path}"
      fbytes = File.read(fpath)
      fhash = Digest::SHA1.digest(fbytes).hexstring
      lsize = leader_sizes[path]? || -1
      fhash.should eq(lhash), "follower file #{path} diverged: follower=#{fbytes.bytesize}B leader_logical=#{lsize}B"
    end
  ensure
    FileUtils.rm_rf follower_dir if follower_dir
  end

  # Control: follower fully syncs BEFORE any publishing. Same oracle must pass,
  # proving the divergence above is caused by joining during publishing, not by
  # the comparison itself.
  it "CONTROL: follower synced before publishing matches the leader exactly" do
    follower_dir = File.tempname
    Dir.mkdir follower_dir
    config = LavinMQ::Config.instance
    replicator = LavinMQ::Clustering::Server.new(config, NullCoordinator.new, 0)
    tcp_server = TCPServer.new("localhost", 0)
    port = tcp_server.local_address.port
    spawn(replicator.listen(tcp_server), name: "repli server spec")

    leader_hashes = Hash(String, String).new
    leader_sizes = Hash(String, Int64).new
    with_amqp_server(replicator: replicator) do |s|
      follower_config = config.dup.tap &.data_dir = follower_dir
      repli = LavinMQ::Clustering::Client.new(follower_config, 1, replicator.password, proxy: false)
      follower_done = WaitGroup.new(1)
      spawn(name: "follower spec") do
        repli.follow("localhost", port)
        follower_done.done
      end
      wait_for { replicator.followers.first?.try &.synced? }

      with_channel(s) do |ch|
        q = ch.queue("race", durable: true)
        2000.times { q.publish("y" * 1000, props: AMQP::Client::Properties.new(delivery_mode: 2_u8)) }
      end

      wait_for { replicator.followers.first?.try { |f| f.synced? && f.lag_in_bytes == 0 } }
      sleep 0.2.seconds
      replicator.files_with_hash do |path, hash|
        leader_hashes[path] = hash.hexstring
        leader_sizes[path] = replicator.with_file(path, nil) { |_f, sz| sz }
      end
      repli.close
      follower_done.wait
    end
    replicator.close

    leader_hashes.each do |path, lhash|
      fpath = File.join(follower_dir, path)
      File.exists?(fpath).should be_true, "follower missing #{path}"
      fhash = Digest::SHA1.digest(File.read(fpath)).hexstring
      fhash.should eq(lhash), "follower file #{path} diverged (oracle false positive?)"
    end
  ensure
    FileUtils.rm_rf follower_dir if follower_dir
  end
end
