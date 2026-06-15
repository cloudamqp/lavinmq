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

  it "refuses to let the view (the fencing token) go backwards" do
    with_tmp_dir do |dir|
      s = VRState.load(dir)
      s.save(view: 5u64, op: 5u64, commit_op: 5u64)
      expect_raises(LavinMQ::Clustering::VR::Error, /view must not decrease/) { s.save(view: 4u64) }
    end
  end

  it "allows op/commit_op to decrease (a full_sync may truncate an uncommitted tail)" do
    with_tmp_dir do |dir|
      s = VRState.load(dir)
      s.save(view: 5u64, op: 9u64, commit_op: 7u64)
      s.save(op: 4u64, commit_op: 4u64) # truncated to a less-complete leader
      reloaded = VRState.load(dir)
      reloaded.op.should eq 4
      reloaded.commit_op.should eq 4
      reloaded.view.should eq 5
    end
  end

  it "advances op without an fsync (durability deferred to the caller's syncfs)" do
    with_tmp_dir do |dir|
      s = VRState.load(dir)
      s.save_op_pending(11u64, 10u64)
      s.op.should eq 11
      VRState.load(dir).op.should eq 11 # still written atomically, just not fsync'd here
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
