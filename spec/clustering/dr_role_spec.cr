require "../spec_helper"
require "../../src/lavinmq/clustering/controller"

# The region's role (primary vs DR follower) is implied by the
# {prefix}/upstream_etcd key in its own etcd, with the config option as a
# fallback. Clearing the key promotes a region to primary; setting it to a
# foreign region's etcd makes it a DR follower of that region.
describe LavinMQ::Clustering::Controller, tags: "etcd" do
  add_etcd_around_each

  describe "#effective_upstream" do
    it "is empty (primary) when neither the key nor the fallback is set" do
      with_datadir do |data_dir|
        config = LavinMQ::Config.instance.dup
        config.data_dir = data_dir
        config.clustering_etcd_prefix = "regionB"
        controller = LavinMQ::Clustering::Controller.new(config, LavinMQ::Etcd.new("localhost:12379"))
        controller.effective_upstream.should eq ""
      end
    end

    it "falls back to the config option when the key is absent" do
      with_datadir do |data_dir|
        config = LavinMQ::Config.instance.dup
        config.data_dir = data_dir
        config.clustering_etcd_prefix = "regionB"
        config.clustering_upstream_etcd_endpoints = "etcd-a1:2379,etcd-a2:2379"
        controller = LavinMQ::Clustering::Controller.new(config, LavinMQ::Etcd.new("localhost:12379"))
        controller.effective_upstream.should eq "etcd-a1:2379,etcd-a2:2379"
      end
    end

    it "uses the etcd key (DR mode) and lets it override the fallback" do
      with_datadir do |data_dir|
        etcd = LavinMQ::Etcd.new("localhost:12379")
        config = LavinMQ::Config.instance.dup
        config.data_dir = data_dir
        config.clustering_etcd_prefix = "regionB"
        config.clustering_upstream_etcd_endpoints = "from-config:2379"
        controller = LavinMQ::Clustering::Controller.new(config, etcd)

        etcd.put("regionB/upstream_etcd", "etcd-a1:2379")
        controller.effective_upstream.should eq "etcd-a1:2379"

        # The "primary" sentinel (failover/promotion) overrides the fallback
        etcd.put("regionB/upstream_etcd", LavinMQ::Clustering::Controller::UPSTREAM_PRIMARY)
        controller.effective_upstream.should eq ""
      end
    end
  end
end
