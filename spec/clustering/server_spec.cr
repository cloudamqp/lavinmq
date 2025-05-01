require "../spec_helper"
require "./spec_helper"
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
        algo = Digest::SHA1.new
        server.files_with_hash(algo) do |_path, hash|
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
        algo = Digest::SHA1.new
        server.files_with_hash(algo) do |_path, hash|
          hash.should eq Digest::SHA1.new.update("foo").final
        end
      ensure
        file.try &.delete
      end
    end
  end
end
