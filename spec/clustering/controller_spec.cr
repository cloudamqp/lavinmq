require "../spec_helper"
require "../../src/lavinmq/clustering/controller"

describe LavinMQ::Clustering::Controller do
  add_etcd_around_each

  describe "sequential ID generation" do
    it "assigns sequential IDs starting from 1" do
      config = LavinMQ::Config.new
      config.data_dir = File.tempname
      Dir.mkdir_p config.data_dir
      config.clustering_etcd_endpoints = "localhost:12379"
      etcd = LavinMQ::Etcd.new("localhost:12379")
      controller = LavinMQ::Clustering::Controller.new(config, etcd)
      controller.id.should eq 1
    ensure
      FileUtils.rm_rf(config.data_dir) if config
    end

    it "skips IDs already claimed in etcd" do
      etcd = LavinMQ::Etcd.new("localhost:12379")
      prefix = LavinMQ::Config.instance.clustering_etcd_prefix
      # Pre-claim IDs 1 and 2
      etcd.put("#{prefix}/replica/1/insync", "1")
      etcd.put("#{prefix}/replica/2/insync", "0")

      config = LavinMQ::Config.new
      config.data_dir = File.tempname
      Dir.mkdir_p config.data_dir
      config.clustering_etcd_endpoints = "localhost:12379"
      controller = LavinMQ::Clustering::Controller.new(config, etcd)
      controller.id.should eq 3
    ensure
      FileUtils.rm_rf(config.data_dir) if config
    end

    it "reuses ID from existing .clustering_id file" do
      config = LavinMQ::Config.new
      config.data_dir = File.tempname
      Dir.mkdir_p config.data_dir
      config.clustering_etcd_endpoints = "localhost:12379"
      # Write a pre-existing clustering ID
      File.write(File.join(config.data_dir, ".clustering_id"), 5.to_s(36))

      etcd = LavinMQ::Etcd.new("localhost:12379")
      controller = LavinMQ::Clustering::Controller.new(config, etcd)
      controller.id.should eq 5
    ensure
      FileUtils.rm_rf(config.data_dir) if config
    end
  end
end
