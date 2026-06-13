require "../spec_helper"
require "../../src/lavinmq/clustering/server"
require "../../src/lavinmq/clustering/etcd_coordinator"
require "lz4"

# Drives the clustering handshake + the two full-sync passes (requesting no
# files) so the follower reaches the Synced state on the leader. Returns the
# socket and the LZ4 reader positioned at the start of the change stream.
private def sync_fake_follower(server, port, id : Int32) : {TCPSocket, Compress::LZ4::Reader}
  io = TCPSocket.new("localhost", port)
  io.write LavinMQ::Clustering::Start
  io.write_bytes server.password.bytesize.to_u8, IO::ByteFormat::LittleEndian
  io.write server.password.to_slice
  io.read_byte # password-accepted byte
  io.write_bytes id, IO::ByteFormat::LittleEndian
  io.flush
  lz4 = Compress::LZ4::Reader.new(io)
  sha1_size = Digest::SHA1.new.digest_size
  2.times do
    loop do
      len = lz4.read_bytes Int32, IO::ByteFormat::LittleEndian
      break if len.zero?
      lz4.skip len
      lz4.skip sha1_size
    end
    io.write_bytes 0i32 # request no files
    io.flush
  end
  {io, lz4}
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
          NullCoordinator.new,
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
      it "should open and read file calculating hash", tags: "slow" do
        data_dir = LavinMQ::Config.instance.data_dir
        Dir.mkdir_p(data_dir)
        server = LavinMQ::Clustering::Server.new(
          LavinMQ::Config.instance,
          NullCoordinator.new,
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

  describe "#followers" do
    it "returns only synced followers" do
      data_dir = LavinMQ::Config.instance.data_dir
      Dir.mkdir_p(data_dir)
      server = LavinMQ::Clustering::Server.new(
        LavinMQ::Config.instance,
        NullCoordinator.new,
        0)
      fi = FakeFileIndex.new(data_dir)
      sock_a, client_a = FakeSocket.pair
      sock_b, client_b = FakeSocket.pair
      synced = LavinMQ::Clustering::Follower.new(sock_a, data_dir, fi)
      syncing = LavinMQ::Clustering::Follower.new(sock_b, data_dir, fi)
      synced.mark_synced!
      server.@followers << synced << syncing # syncing left in Syncing state

      server.followers.should eq [synced] # excludes the still-syncing follower
    ensure
      sock_a.try &.close
      client_a.try &.close
      sock_b.try &.close
      client_b.try &.close
      FileUtils.rm_rf LavinMQ::Config.instance.data_dir
    end
  end

  describe "#append" do
    it "raises ArgumentError when no MFile is registered for the path" do
      data_dir = LavinMQ::Config.instance.data_dir
      Dir.mkdir_p(data_dir)
      server = LavinMQ::Clustering::Server.new(
        LavinMQ::Config.instance,
        NullCoordinator.new,
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

  describe "join baseline (full_sync cut)" do
    it "streams only the unsynced tail of a record that straddles the cut" do
      data_dir = LavinMQ::Config.instance.data_dir
      Dir.mkdir_p(data_dir)
      server = LavinMQ::Clustering::Server.new(
        LavinMQ::Config.instance,
        NullCoordinator.new,
        0)
      tcp_server = TCPServer.new("localhost", 0)
      spawn(server.listen(tcp_server), name: "repli server spec")

      path = File.join(data_dir, "baseline_seg")
      mfile = MFile.new(path, 1024)
      mfile.print "head" # a complete 4-byte record
      server.register_file(mfile)
      # A writer is mid-record: it has written the first half of the record at
      # [4, 12) but not the rest, and the dispatch only happens once the whole
      # record is written. The join takes its cut from the live size (8), so
      # the cut lands inside the record.
      mfile.print "part"

      io, lz4 = sync_fake_follower(server, tcp_server.local_address.port, 5)
      wait_for { server.followers.any? &.id.== 5 }

      # The writer finishes the record and dispatches it as a whole.
      mfile.print "more"
      server.append(path, 0, 4) # entirely within the baseline → skipped
      server.append(path, 4, 8) # straddles the cut → only the tail is sent

      received = Channel({String, Int64, String}).new(1)
      spawn do
        len = lz4.read_bytes Int32, IO::ByteFormat::LittleEndian
        filename = lz4.read_string(len)
        size = lz4.read_bytes Int64, IO::ByteFormat::LittleEndian
        body = lz4.read_string(size.abs.to_i32)
        received.send({filename, size, body})
      rescue IO::Error
      end

      select
      when msg = received.receive
        msg[0].should eq "baseline_seg"
        # Only the bytes full_sync didn't deliver: not the whole record (would
        # duplicate "part") and not nothing (would lose "more").
        msg[1].should eq(-4i64)
        msg[2].should eq "more"
      when timeout(3.seconds)
        fail "the straddling record's tail was never streamed to the joined follower"
      end
    ensure
      io.try &.close
      mfile.try &.close
      server.try &.close
      tcp_server.try &.close
      FileUtils.rm_rf LavinMQ::Config.instance.data_dir
    end
  end

  describe "thread safety" do
    it "concurrent files_with_hash and mutations don't crash", tags: "slow" do
      data_dir = LavinMQ::Config.instance.data_dir
      Dir.mkdir_p(data_dir)
      server = LavinMQ::Clustering::Server.new(
        LavinMQ::Config.instance,
        NullCoordinator.new,
        0)

      mfiles = Array(MFile).new
      10.times do |i|
        mfile = MFile.new(File.join(data_dir, "concurrent_test_#{i}"), 1024)
        mfile.print "content_#{i}"
        server.register_file(mfile)
        mfiles << mfile
      end

      done = Channel(Nil).new
      iterations = 200_000

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
        when 2 then server.append_bytes(mfile.path, "data".to_slice, 0i64)
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

    it "concurrent with_file and mutations don't crash", tags: "slow" do
      data_dir = LavinMQ::Config.instance.data_dir
      Dir.mkdir_p(data_dir)
      server = LavinMQ::Clustering::Server.new(
        LavinMQ::Config.instance,
        NullCoordinator.new,
        0)

      mfiles = Array(MFile).new
      10.times do |i|
        mfile = MFile.new(File.join(data_dir, "concurrent_wf_#{i}"), 1024)
        mfile.print "content_#{i}"
        server.register_file(mfile)
        mfiles << mfile
      end

      done = Channel(Nil).new
      iterations = 200_000

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
