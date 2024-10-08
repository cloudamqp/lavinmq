require "spec"
{% if flag?(:windows) %}
  require "../src/lavinmq/mfile_windows"
{% else %}
  require "../src/lavinmq/mfile"
{% end %}

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
end
