require "../spec_helper"
require "../../src/lavinmq/clustering/server"
require "../../src/lavinmq/clustering/etcd_coordinator"

# Build a Server with two synced followers backed by FakeSocket pairs and drive
# their acks directly, exercising wait_for_followers_ack aggregation.
private def with_two_synced_followers(&)
  data_dir = LavinMQ::Config.instance.data_dir
  Dir.mkdir_p(data_dir)
  server = LavinMQ::Clustering::Server.new(
    LavinMQ::Config.instance,
    LavinMQ::Etcd.new("localhost:12379"),
    0)
  fi = FakeFileIndex.new(data_dir)
  sock_a, client_a = FakeSocket.pair
  sock_b, client_b = FakeSocket.pair
  a = LavinMQ::Clustering::Follower.new(sock_a, data_dir, fi)
  b = LavinMQ::Clustering::Follower.new(sock_b, data_dir, fi)
  a.mark_synced!
  b.mark_synced!
  server.@followers << a << b
  # Drain both client sides so the synchronous flush in wait_for_confirm
  # doesn't block on socket writes.
  {client_a, client_b}.each do |c|
    spawn do
      buf = uninitialized UInt8[4096]
      loop { c.read(buf.to_slice) }
    rescue IO::Error
    end
  end
  # Replicate some bytes to both followers (so they have outstanding lag).
  # Each test starts the ack loops itself, so it controls the ack deadline.
  a.append("#{data_dir}/wf", "hello".to_slice)
  b.append("#{data_dir}/wf", "hello".to_slice)
  yield server, a, b, client_a, client_b
ensure
  sock_a.try &.close
  client_a.try &.close
  sock_b.try &.close
  client_b.try &.close
  FileUtils.rm_rf LavinMQ::Config.instance.data_dir
end

describe LavinMQ::Clustering::Server, tags: "etcd" do
  add_etcd_around_each

  describe "#files_with_hash" do
    describe "for MFile" do
      it "should hash only the real (data) size, not the sparse capacity" do
        data_dir = LavinMQ::Config.instance.data_dir
        Dir.mkdir_p(data_dir)
        server = LavinMQ::Clustering::Server.new(
          LavinMQ::Config.instance,
          LavinMQ::Clustering::EtcdCoordinator.new(LavinMQ::Config.instance, LavinMQ::Etcd.new("localhost:12379")),
          0)
        file = MFile.new(File.join(data_dir, "mfile_hash_test"), 1024)
        file.print "foo"
        server.register_file(file)
        server.files_with_hash do |_path, hash|
          hash.should eq Digest::SHA1.new.update("foo").final
        end
      ensure
        file.try &.delete
        FileUtils.rm_rf LavinMQ::Config.instance.data_dir
      end
    end

    describe "for File" do
      it "should open and read file calculating hash" do
        data_dir = LavinMQ::Config.instance.data_dir
        Dir.mkdir_p(data_dir)
        server = LavinMQ::Clustering::Server.new(
          LavinMQ::Config.instance,
          LavinMQ::Clustering::EtcdCoordinator.new(LavinMQ::Config.instance, LavinMQ::Etcd.new("localhost:12379")),
          0)
        path = File.join(data_dir, "file_hash_test")
        file = File.open(path, "w")
        file.print "foo"
        file.close
        server.register_file(file)
        server.files_with_hash do |_path, hash|
          hash.should eq Digest::SHA1.new.update("foo").final
        end
      ensure
        file.try &.delete
        FileUtils.rm_rf LavinMQ::Config.instance.data_dir
      end
    end
  end

  describe "#wait_for_followers_ack" do
    it "returns true when every synced follower acks" do
      with_two_synced_followers do |server, a, b, client_a, client_b|
        spawn { a.ack_loop }
        spawn { b.ack_loop }
        client_a.write_bytes a.lag_in_bytes, IO::ByteFormat::LittleEndian
        client_b.write_bytes b.lag_in_bytes, IO::ByteFormat::LittleEndian
        server.wait_for_followers_ack.should be_true
      end
    end

    it "returns false (leader falls back to syncfs) when one follower fails to ack" do
      with_two_synced_followers do |server, a, b, client_a, _client_b|
        # Follower A acks; B stays connected but never acks and is dropped by
        # ack_loop after its short ack deadline.
        spawn { a.ack_loop }
        spawn { b.ack_loop(50.milliseconds) }
        client_a.write_bytes a.lag_in_bytes, IO::ByteFormat::LittleEndian
        server.wait_for_followers_ack.should be_false
      end
    end
  end

  describe "#append" do
    it "raises ArgumentError when no MFile is registered for the path" do
      data_dir = LavinMQ::Config.instance.data_dir
      Dir.mkdir_p(data_dir)
      server = LavinMQ::Clustering::Server.new(
        LavinMQ::Config.instance,
        LavinMQ::Clustering::EtcdCoordinator.new(LavinMQ::Config.instance, LavinMQ::Etcd.new("localhost:12379")),
        0)
      path = File.join(data_dir, "path_only_file")
      File.write(path, "data")
      server.register_file(path) # path-only, not MFile

      expect_raises(ArgumentError, /requires an MFile/) do
        server.append(path, 0, 4)
      end
    ensure
      FileUtils.rm_rf LavinMQ::Config.instance.data_dir
    end
  end

  describe "thread safety" do
    it "concurrent files_with_hash and mutations don't crash" do
      data_dir = LavinMQ::Config.instance.data_dir
      Dir.mkdir_p(data_dir)
      server = LavinMQ::Clustering::Server.new(
        LavinMQ::Config.instance,
        LavinMQ::Clustering::EtcdCoordinator.new(LavinMQ::Config.instance, LavinMQ::Etcd.new("localhost:12379")),
        0)

      mfiles = Array(MFile).new
      10.times do |i|
        mfile = MFile.new(File.join(data_dir, "concurrent_test_#{i}"), 1024)
        mfile.print "content_#{i}"
        server.register_file(mfile)
        mfiles << mfile
      end

      done = Channel(Nil).new
      iterations = 1_000_000

      # Reader on a separate thread: iterate files_with_hash
      Fiber::ExecutionContext::Isolated.new("test-concurrent-hash") do
        iterations.times do
          server.files_with_hash { |_path, _hash| }
        end
        done.send nil
      end

      # Writer on main thread: mix of mutations
      iterations.times do |i|
        mfile = mfiles[i % mfiles.size]
        case i % 5
        when 0 then server.delete_file(mfile.path)
        when 1 then server.replace_file(mfile.path)
        when 2 then server.append(mfile.path, "data".to_slice)
        else        server.register_file(mfile)
        end
      end

      select
      when done.receive
      when timeout(30.seconds)
        fail "Timed out waiting for concurrent operations"
      end

      server.nr_of_files.should be >= 0
    ensure
      mfiles.try &.each { |mf| mf.delete rescue nil }
      FileUtils.rm_rf LavinMQ::Config.instance.data_dir
    end

    it "concurrent with_file and mutations don't crash" do
      data_dir = LavinMQ::Config.instance.data_dir
      Dir.mkdir_p(data_dir)
      server = LavinMQ::Clustering::Server.new(
        LavinMQ::Config.instance,
        LavinMQ::Clustering::EtcdCoordinator.new(LavinMQ::Config.instance, LavinMQ::Etcd.new("localhost:12379")),
        0)

      mfiles = Array(MFile).new
      10.times do |i|
        mfile = MFile.new(File.join(data_dir, "concurrent_wf_#{i}"), 1024)
        mfile.print "content_#{i}"
        server.register_file(mfile)
        mfiles << mfile
      end

      done = Channel(Nil).new
      iterations = 1_000_000

      # Reader on a separate thread: call with_file repeatedly
      Fiber::ExecutionContext::Isolated.new("test-concurrent-wf") do
        iterations.times do |i|
          key = "concurrent_wf_#{i % mfiles.size}"
          server.with_file(key) { |_f, _size| }
        end
        done.send nil
      end

      # Writer on main thread: register and delete files
      iterations.times do |i|
        mfile = mfiles[i % mfiles.size]
        if i % 3 == 0
          server.delete_file(mfile.path)
        else
          server.register_file(mfile)
        end
      end

      select
      when done.receive
      when timeout(30.seconds)
        fail "Timed out waiting for concurrent operations"
      end
    ensure
      mfiles.try &.each { |mf| mf.delete rescue nil }
      FileUtils.rm_rf LavinMQ::Config.instance.data_dir
    end
  end
end
