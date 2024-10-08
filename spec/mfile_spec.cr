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
end
