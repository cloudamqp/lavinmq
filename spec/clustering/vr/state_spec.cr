require "../../spec_helper"
require "../../../src/lavinmq/clustering/vr/state"

alias VRState = LavinMQ::Clustering::VR::State

private def with_tmp_dir(&)
  dir = File.tempname
  Dir.mkdir_p dir
  begin
    yield dir
  ensure
    FileUtils.rm_rf dir
  end
end

describe LavinMQ::Clustering::VR::State do
  it "starts fresh (zeroed) when no file exists" do
    with_tmp_dir do |dir|
      s = VRState.load(dir)
      s.view.should eq 0
      s.op.should eq 0
      s.commit_op.should eq 0
    end
  end

  it "persists and reloads view/op/commit_op" do
    with_tmp_dir do |dir|
      VRState.load(dir).save(view: 7u64, op: 42u64, commit_op: 40u64)
      s = VRState.load(dir)
      s.view.should eq 7
      s.op.should eq 42
      s.commit_op.should eq 40
    end
  end

  it "keeps unspecified values when saving a subset" do
    with_tmp_dir do |dir|
      s = VRState.load(dir)
      s.save(op: 10u64)
      s.save(view: 3u64)
      s.view.should eq 3
      s.op.should eq 10
      VRState.load(dir).op.should eq 10
    end
  end

  it "refuses to let view, op or commit_op go backwards" do
    with_tmp_dir do |dir|
      s = VRState.load(dir)
      s.save(view: 5u64, op: 5u64, commit_op: 5u64)
      expect_raises(LavinMQ::Clustering::VR::Error, /view must not decrease/) { s.save(view: 4u64) }
      expect_raises(LavinMQ::Clustering::VR::Error, /op must not decrease/) { s.save(op: 4u64) }
      expect_raises(LavinMQ::Clustering::VR::Error, /commit_op must not decrease/) { s.save(commit_op: 4u64) }
    end
  end

  it "raises on a corrupt file rather than silently resetting" do
    with_tmp_dir do |dir|
      File.write(File.join(dir, VRState::FILENAME), "garbage that is not a vr_state file")
      expect_raises(LavinMQ::Clustering::VR::Error) do
        VRState.load(dir)
      end
    end
  end

  it "writes atomically, leaving no temp file behind" do
    with_tmp_dir do |dir|
      VRState.load(dir).save(view: 1u64)
      File.exists?(File.join(dir, "#{VRState::FILENAME}.tmp")).should be_false
      File.exists?(File.join(dir, VRState::FILENAME)).should be_true
    end
  end
end
