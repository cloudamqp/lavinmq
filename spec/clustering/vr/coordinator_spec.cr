require "../../spec_helper"
require "../../../src/lavinmq/clustering/vr/coordinator"

alias VRCoordinator = LavinMQ::Clustering::VR::Coordinator

private def with_tmp_dir(&)
  dir = File.tempname
  Dir.mkdir_p dir
  begin
    yield dir
  ensure
    FileUtils.rm_rf dir
  end
end

private def config_with(data_dir : String, secret : String?) : LavinMQ::Config
  c = LavinMQ::Config.new
  c.data_dir = data_dir
  c.clustering_secret = secret
  c
end

describe LavinMQ::Clustering::VR::Coordinator do
  describe "#password" do
    it "reads the secret from .clustering_password when present" do
      with_tmp_dir do |dir|
        path = File.join(dir, ".clustering_password")
        File.write(path, "from-file-secret\n")
        File.chmod(path, 0o600)
        VRCoordinator.new(config_with(dir, "from-config")).password.should eq "from-file-secret"
      end
    end

    it "seeds .clustering_password from clustering_secret on first use" do
      with_tmp_dir do |dir|
        path = File.join(dir, ".clustering_password")
        File.exists?(path).should be_false
        VRCoordinator.new(config_with(dir, "seed-secret")).password.should eq "seed-secret"
        File.read(path).strip.should eq "seed-secret"
        File.info(path).permissions.should eq File::Permissions.new(0o600)
      end
    end

    it "is stable once seeded, even if the config secret later changes" do
      with_tmp_dir do |dir|
        VRCoordinator.new(config_with(dir, "original")).password.should eq "original"
        # File is now authoritative: a different config secret is ignored.
        VRCoordinator.new(config_with(dir, "changed")).password.should eq "original"
      end
    end

    it "raises when neither the file nor a configured secret is available" do
      with_tmp_dir do |dir|
        expect_raises(LavinMQ::Clustering::VR::Error, /No clustering secret/) do
          VRCoordinator.new(config_with(dir, nil)).password
        end
      end
    end

    it "raises on an empty .clustering_password file" do
      with_tmp_dir do |dir|
        path = File.join(dir, ".clustering_password")
        File.write(path, "   \n")
        File.chmod(path, 0o600)
        expect_raises(LavinMQ::Clustering::VR::Error, /empty/) do
          VRCoordinator.new(config_with(dir, nil)).password
        end
      end
    end

    it "refuses to read a group/world-accessible .clustering_password" do
      with_tmp_dir do |dir|
        path = File.join(dir, ".clustering_password")
        File.write(path, "secret")
        File.chmod(path, 0o644) # group/other readable
        expect_raises(LavinMQ::Clustering::VR::Error, /group\/world-accessible/) do
          VRCoordinator.new(config_with(dir, nil)).password
        end
      end
    end

    it "accepts a 0600 .clustering_password" do
      with_tmp_dir do |dir|
        path = File.join(dir, ".clustering_password")
        File.write(path, "secret")
        File.chmod(path, 0o600)
        VRCoordinator.new(config_with(dir, nil)).password.should eq "secret"
      end
    end
  end
end
