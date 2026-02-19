require "../spec_helper"
require "../../src/lavinmq/clustering/server"

describe LavinMQ::Clustering::Server, tags: "etcd" do
  add_etcd_around_each

  describe "#known_replicas" do
    it "returns replicas marked in etcd" do
      with_datadir do |dir|
        config = LavinMQ::Config.instance.dup.tap &.data_dir = dir
        etcd = LavinMQ::Etcd.new("localhost:12379")
        prefix = config.clustering_etcd_prefix
        etcd.put("#{prefix}/replica/1/insync", "1")
        etcd.put("#{prefix}/replica/2/insync", "0")
        server = LavinMQ::Clustering::Server.new(config, etcd, 0)
        replicas = server.known_replicas
        replicas["1"].should be_true
        replicas["2"].should be_false
        server.close
      end
    end
  end

  describe "#forget_replica" do
    it "removes a replica from etcd" do
      with_datadir do |dir|
        config = LavinMQ::Config.instance.dup.tap &.data_dir = dir
        etcd = LavinMQ::Etcd.new("localhost:12379")
        prefix = config.clustering_etcd_prefix
        etcd.put("#{prefix}/replica/5/insync", "0")
        server = LavinMQ::Clustering::Server.new(config, etcd, 0)
        server.forget_replica(5).should be_true
        etcd.get("#{prefix}/replica/5/insync").should be_nil
        server.close
      end
    end

    it "returns false when replica doesn't exist" do
      with_datadir do |dir|
        config = LavinMQ::Config.instance.dup.tap &.data_dir = dir
        server = LavinMQ::Clustering::Server.new(
          config,
          LavinMQ::Etcd.new("localhost:12379"),
          0)
        server.forget_replica(999).should be_false
        server.close
      end
    end
  end

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
end
