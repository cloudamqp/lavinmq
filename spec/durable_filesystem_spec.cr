require "./spec_helper"
require "../src/lavinmq/filesystem"

describe LavinMQ::FileSystem do
  describe ".replace" do
    it "atomically replaces the file without leaving the tmp file" do
      with_datadir do |data_dir|
        path = File.join(data_dir, "file.json")
        File.write(path, "old")
        LavinMQ::FileSystem.replace(path, &.print("new"))
        File.read(path).should eq "new"
        File.exists?("#{path}.tmp").should be_false
      end
    end

    it "keeps the old content and removes the tmp file if the block raises" do
      with_datadir do |data_dir|
        path = File.join(data_dir, "file.json")
        File.write(path, "old")
        expect_raises(Exception, "boom") do
          LavinMQ::FileSystem.replace(path) do |f|
            f.print "partial"
            raise "boom"
          end
        end
        File.read(path).should eq "old"
        File.exists?("#{path}.tmp").should be_false
      end
    end
  end

  describe ".durable_rename" do
    it "renames across directories" do
      with_datadir do |data_dir|
        Dir.mkdir(File.join(data_dir, "a"))
        Dir.mkdir(File.join(data_dir, "b"))
        source = File.join(data_dir, "a", "file")
        destination = File.join(data_dir, "b", "file")
        File.write(source, "data")
        LavinMQ::FileSystem.durable_rename(source, destination)
        File.exists?(source).should be_false
        File.read(destination).should eq "data"
      end
    end

    it "renames an open file in place" do
      with_datadir do |data_dir|
        source = File.join(data_dir, "file.tmp")
        destination = File.join(data_dir, "file")
        File.open(source, "w") do |f|
          f.print "data"
          LavinMQ::FileSystem.durable_rename(f, destination)
          f.path.should eq destination
        end
        File.read(destination).should eq "data"
      end
    end
  end

  describe ".mkdir_p" do
    it "creates missing parent directories and accepts existing ones" do
      with_datadir do |data_dir|
        path = File.join(data_dir, "a", "b", "c")
        LavinMQ::FileSystem.mkdir_p(path)
        Dir.exists?(path).should be_true
        LavinMQ::FileSystem.mkdir_p(path)
        Dir.exists?(path).should be_true
      end
    end
  end
end
