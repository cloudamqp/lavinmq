require "./spec_helper"
require "file_utils"
require "time"
require "../src/lavinmq/message_store"

def mktmpdir(&)
  path = File.tempname
  Dir.mkdir_p(path)
  begin
    yield path
  ensure
    FileUtils.rm_r(path)
  end
end

describe LavinMQ::MessageStore do
  it "deletes orphaned ack files" do
    mktmpdir do |dir|
      # Create a dummy msgs file
      File.write(File.join(dir, "msgs.0000000001"), "")
      # Create a corresponding acks file
      File.write(File.join(dir, "acks.0000000001"), "data")
      # Create an orphaned acks file
      File.write(File.join(dir, "acks.0000000002"), "data")

      store = LavinMQ::QueueMessageStore.new(dir, nil)
      store.close

      File.exists?(File.join(dir, "acks.0000000001")).should be_true
      File.exists?(File.join(dir, "acks.0000000002")).should be_false
    end
  end
end
