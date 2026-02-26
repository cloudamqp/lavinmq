require "../spec_helper"
require "../../src/lavinmq/clustering/server"

describe LavinMQ::Clustering::Server, tags: "etcd" do
  add_etcd_around_each

  describe "#files_with_hash" do
    describe "for MFile" do
      it "should use mfile buffer when calculating hash" do
        server = LavinMQ::Clustering::Server.new(
          LavinMQ::Config.instance,
          LavinMQ::Etcd.new("localhost:12379"),
          0)
        file = MFile.new(File.tempname, 1024)
        file.print "foo"
        server.register_file(file)
        server.files_with_hash do |_path, hash|
          hash.should eq Digest::SHA1.new.update("foo").final
        end
      ensure
        file.try &.delete
      end
    end

    describe "for File" do
      it "should open and read file calculating hash" do
        server = LavinMQ::Clustering::Server.new(
          LavinMQ::Config.instance,
          LavinMQ::Etcd.new("localhost:12379"),
          0)
        file = File.open(File.tempname, "w")
        file.print "foo"
        file.close
        server.register_file(file)
        server.files_with_hash do |_path, hash|
          hash.should eq Digest::SHA1.new.update("foo").final
        end
      ensure
        file.try &.delete
      end
    end
  end

  describe "thread safety" do
    it "concurrent files_with_hash and mutations don't crash" do
      data_dir = LavinMQ::Config.instance.data_dir
      Dir.mkdir_p(data_dir)
      server = LavinMQ::Clustering::Server.new(
        LavinMQ::Config.instance,
        LavinMQ::Etcd.new("localhost:12379"),
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
        when 0 then server.delete_file(mfile.path, WaitGroup.new(0))
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
        LavinMQ::Etcd.new("localhost:12379"),
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
          server.with_file(key) { |_f| }
        end
        done.send nil
      end

      # Writer on main thread: register and delete files
      iterations.times do |i|
        mfile = mfiles[i % mfiles.size]
        if i % 3 == 0
          server.delete_file(mfile.path, WaitGroup.new(0))
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
