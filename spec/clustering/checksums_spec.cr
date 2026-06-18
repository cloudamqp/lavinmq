require "../spec_helper"
require "../../src/lavinmq/clustering/checksums"

describe LavinMQ::Clustering::Checksums do
  it "persists an appended hash immediately, before any store/close" do
    with_datadir do |data_dir|
      checksums = LavinMQ::Clustering::Checksums.new(data_dir)
      hash = Digest::SHA1.digest("hello")
      checksums.append("queue1/msgs.0000000001", hash)

      path = File.join(data_dir, "checksums.sha1")
      File.exists?(path).should be_true
      File.read(path).should eq "#{hash.hexstring} *queue1/msgs.0000000001\n"
    end
  end

  it "restores appended hashes once, then discards them (one-shot)" do
    with_datadir do |data_dir|
      hash = Digest::SHA1.digest("hello")
      written = LavinMQ::Clustering::Checksums.new(data_dir)
      written.append("queue1/msgs.0000000001", hash)

      restored = LavinMQ::Clustering::Checksums.new(data_dir)
      restored.restore
      restored["queue1/msgs.0000000001"]?.should eq hash

      # one-shot: the on-disk copy is discarded after restore, so a stale hash
      # can't outlive a 2nd crash before a clean store rewrites it.
      File.size(File.join(data_dir, "checksums.sha1")).should eq 0
      again = LavinMQ::Clustering::Checksums.new(data_dir)
      again.restore
      again["queue1/msgs.0000000001"]?.should be_nil
    end
  end

  it "rewrites a clean snapshot on store" do
    with_datadir do |data_dir|
      checksums = LavinMQ::Clustering::Checksums.new(data_dir)
      checksums.append("a", Digest::SHA1.digest("a"))
      checksums.append("b", Digest::SHA1.digest("b"))
      checksums.store

      lines = File.read(File.join(data_dir, "checksums.sha1")).lines
      lines.size.should eq checksums.size
      lines.each(&.should(match(/^[0-9a-f]{40} \*/)))
      # No torn temp file left behind by the atomic rename.
      File.exists?(File.join(data_dir, "checksums.sha1.tmp")).should be_false
    end
  end

  it "keeps persisting via append after a store rewrite" do
    with_datadir do |data_dir|
      checksums = LavinMQ::Clustering::Checksums.new(data_dir)
      checksums.append("a", Digest::SHA1.digest("a"))
      checksums.store # rewrites and adopts the new handle
      checksums.append("b", Digest::SHA1.digest("b"))

      restored = LavinMQ::Clustering::Checksums.new(data_dir)
      restored.restore
      restored["a"]?.should eq Digest::SHA1.digest("a")
      restored["b"]?.should eq Digest::SHA1.digest("b")
    end
  end
end
