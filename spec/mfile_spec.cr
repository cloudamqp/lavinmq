require "spec"
require "../src/lavinmq/mfile"

describe MFile do
  it "can be double closed" do
    file = File.tempfile "mfile_spec"
    begin
      mfile = MFile.new file.path
      mfile.close
      mfile.close
    ensure
      file.delete
    end
  end

  it "can be read" do
    file = File.tempfile "mfile_spec"
    file.print "hello world"
    file.flush
    begin
      MFile.open(file.path) do |mfile|
        buf = Bytes.new(6)
        cnt = mfile.read(buf)
        String.new(buf[0, cnt]).should eq "hello "
        cnt = mfile.read(buf)
        String.new(buf[0, cnt]).should eq "world"
      end
    ensure
      file.delete
    end
  end

  describe "#resize" do
    it "can increase size" do
      file = File.tempfile "mfile_spec"
      file.print "hello world"
      file.flush
      data = "foo"
      initial_size = file.size
      MFile.open(file.path, initial_size) do |mfile|
        mfile.capacity.should eq initial_size
        expect_raises(IO::EOFError) { mfile.write data.to_slice }
        mfile.resize(mfile.size + data.bytesize)
        mfile.write data.to_slice
        mfile.capacity.should eq(initial_size + data.bytesize)
      end
      file.size.should eq(initial_size + data.bytesize)
      data = File.read(file.path)
      data.should eq "hello worldfoo"
    ensure
      file.try &.delete
    end
  end
end
