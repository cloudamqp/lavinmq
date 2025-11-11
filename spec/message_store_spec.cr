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

def with_store(&)
  mktmpdir do |dir|
    store = LavinMQ::MessageStore.new(dir, nil, durable: true)
    begin
      yield store, dir
    ensure
      store.close
    end
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

      store = LavinMQ::MessageStore.new(dir, nil)
      store.close

      File.exists?(File.join(dir, "acks.0000000001")).should be_true
      File.exists?(File.join(dir, "acks.0000000002")).should be_false
    end
  end

  # Verifies #1296
  it "deletes unused segments on startup when multiple 'empty' segments exist" do
    with_store do |store, dir|
      msg_size = LavinMQ::Config.instance.segment_size.to_u64 - (LavinMQ::BytesMessage::MIN_BYTESIZE + 5)
      msg = LavinMQ::Message.new(RoughTime.unix_ms, "e", "k", AMQ::Protocol::Properties.new, msg_size, IO::Memory.new("a" * msg_size))

      5.times { store.push(msg) }
      store.@segments.size.should eq 5
      store.close
      wait_for { store.closed }

      # Write ack files
      store.@segments.keys.each do |seg|
        break if seg == store.@segments.keys.last # don't ack the last segment
        path = File.join(dir, "acks." + seg.to_s.rjust(10, '0'))
        File.open(path, "w") do |f|
          f.write_bytes(4u32) # only one message per segment
        end
      end

      store = LavinMQ::MessageStore.new(dir, nil, durable: true)
      segment_files = Dir.glob(File.join(dir, "msgs.*")).count &.match(/msgs.\d{10}$/)
      store.@segments.size.should eq 1
      segment_files.should eq 1
    end
  end

  it "first? should return nil from empty segment" do
    mktmpdir do |dir|
      File.write(File.join(dir, "msgs.0000000001"), "")
      store = LavinMQ::MessageStore.new(dir, nil)
      store.@segments.first_value.truncate(1000)
      store.first?.should be_nil
      store.close
    end
  end

  it "shift? should return nil from empty segment" do
    mktmpdir do |dir|
      File.write(File.join(dir, "msgs.0000000001"), "")
      store = LavinMQ::MessageStore.new(dir, nil)
      store.@segments.first_value.truncate(1000)
      store.shift?.should be_nil
      store.close
    end
  end

  it "can ack messages after restart" do
    mktmpdir do |dir|
      File.write(File.join(dir, "msgs.0000000001"), "")
      File.write(File.join(dir, "acks.0000000001"), "\x00\x00\x00\x04")
      store = LavinMQ::MessageStore.new(dir, nil)
      body_io = IO::Memory.new("hello")
      message = LavinMQ::Message.new(RoughTime.unix_ms, "test_exchange", "test_key", AMQ::Protocol::Properties.new, 5u64, body_io)
      store.push(message)
      env = store.shift?.should_not be_nil
      String.new(env.message.body).should eq "hello"
      store.delete(env.segment_position)
      store.close
    end
  end

  describe "#empty?" do
    it "should be true for a fresh store" do
      with_store do |store|
        store.empty?.should be_true
      end
    end

    it "should be false after push" do
      with_store do |store|
        store.push LavinMQ::Message.new("ex", "rk", "body")
        store.empty?.should be_false
      end
    end

    it "should be true after all messages has been shifted" do
      with_store do |store|
        5.times { store.push LavinMQ::Message.new("ex", "rk", "body") }

        while store.shift?
        end

        store.empty?.should be_true
      end
    end
  end

  describe "#empty" do
    it "should be true for a new store" do
      with_store do |store|
        store.empty.should be_true
      end
    end

    it "should be true after all messages has been shifted" do
      with_store do |store|
        5.times { store.push LavinMQ::Message.new("ex", "rk", "body") }

        while store.shift?
        end

        store.empty.should be_true
      end
    end

    it "should be false on after a push to a new store" do
      with_store do |store|
        store.push LavinMQ::Message.new("ex", "rk", "body")

        store.empty.should be_false
      end
    end

    it "should be false after push to an empty store" do
      with_store do |store|
        5.times { store.push LavinMQ::Message.new("ex", "rk", "body") }

        while store.shift?
        end

        5.times { store.push LavinMQ::Message.new("ex", "rk", "body") }

        store.empty.should be_false
      end
    end

    it "should be false after requeue" do
      with_store do |store|
        store.push LavinMQ::Message.new("ex", "rk", "body")
        env = store.shift?.should_not be_nil
        store.empty.should be_true
        store.requeue env.segment_position
        store.empty.should be_false
      end
    end

    it "should be true after last message has been purged" do
      with_store do |store|
        5.times { store.push LavinMQ::Message.new("ex", "rk", "body") }

        store.empty.should be_false
        store.purge(10)

        store.empty.should be_true
      end
    end

    it "should be true after purge_all" do
      with_store do |store|
        5.times { store.push LavinMQ::Message.new("ex", "rk", "body") }

        store.empty.should be_false
        store.purge_all

        store.empty.should be_true
      end
    end
  end
end
