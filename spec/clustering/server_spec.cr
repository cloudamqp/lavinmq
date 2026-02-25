require "../spec_helper"
require "../../src/lavinmq/clustering/server"

describe LavinMQ::Clustering::Server do
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

    it "should skip MFiles that are closed during iteration" do
      data_dir = LavinMQ::Config.instance.data_dir
      server = LavinMQ::Clustering::Server.new(
        LavinMQ::Config.instance,
        LavinMQ::Etcd.new("localhost:12379"),
        0)
      mfile1 = MFile.new(File.join(data_dir, "test1"), 1024)
      mfile1.print "foo"
      server.register_file(mfile1)

      mfile2 = MFile.new(File.join(data_dir, "test2"), 1024)
      mfile2.print "bar"
      server.register_file(mfile2)

      # Simulate queue churn: when the block is called for the first
      # file, close the second MFile. Without the fix, the iteration
      # would raise IO::Error("Closed mfile") on the next file.
      yielded_paths = [] of String
      first = true
      server.files_with_hash do |path, _hash|
        if first
          first = false
          mfile2.close
        end
        yielded_paths << path
      end

      yielded_paths.size.should eq 1
    ensure
      mfile1.try &.delete
      mfile2.try &.delete
    end
  end

  describe "#with_file" do
    it "should yield nil for a closed MFile" do
      data_dir = LavinMQ::Config.instance.data_dir
      server = LavinMQ::Clustering::Server.new(
        LavinMQ::Config.instance,
        LavinMQ::Etcd.new("localhost:12379"),
        0)
      mfile = MFile.new(File.join(data_dir, "test_closed"), 1024)
      mfile.print "foo"
      server.register_file(mfile)
      mfile.close

      server.with_file("test_closed") do |f|
        f.should be_nil
      end
    ensure
      mfile.try &.delete
    end
  end
end
