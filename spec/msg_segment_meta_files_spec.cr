require "./spec_helper"

describe "Message segment metadata files" do
  describe "Metadata file creation" do
    it "creates meta file when segment becomes full" do
      with_datadir do |dir|
        store = LavinMQ::MessageStore.new(dir, nil)

        # Create a large message to fill segments faster
        segment_size = LavinMQ::Config.instance.segment_size
        large_message = "x" * (segment_size // 4) # message is 1/4 of segment size

        # Publish enough messages to create multiple segments
        10.times do |i|
          store.push(LavinMQ::Message.new("", "rk", "#{large_message}_#{i}"))
        end
        store.@segments.size.should be > 1

        # Check that meta files exist for completed segments
        meta_files = 0
        store.@segments.each do |seg_id, mfile|
          next if seg_id == store.@segments.last_key # skip current writing segment
          meta_path = mfile.path.sub("msgs.", "meta.")
          if File.exists?(meta_path)
            meta_files += 1
            # Verify meta file contains message count
            count = File.open(meta_path, &.read_bytes(UInt32))
            count.should eq store.@segment_msg_count[seg_id]
          else
            fail "Meta file #{meta_path} should exist"
          end
        end
        meta_files.should eq store.@segments.size - 1

        store.close
      end
    end

    it "writes correct metadata for stream queues" do
      with_datadir do |dir|
        store = LavinMQ::AMQP::StreamMessageStore.new(dir, nil)

        # Publish enough messages to trigger new segment creation
        segment_size = LavinMQ::Config.instance.segment_size
        body = "x" * 256
        messages_needed = (segment_size / body.bytesize).to_i + 50

        messages_needed.times do |i|
          store.push(LavinMQ::Message.new("", "rk", "#{body}#{i}"))
        end

        # Should have created new segments
        store.@segments.size.should be > 1

        # Check meta file exists and contains stream-specific data
        store.@segments.each do |seg_id, mfile|
          next if seg_id == store.@segments.last_key # skip current writing segment
          meta_path = mfile.path.sub("msgs.", "meta.")
          File.exists?(meta_path).should be_true

          # Verify meta file format for stream queue (count + offset + timestamp)
          File.open(meta_path) do |f|
            count = f.read_bytes(UInt32)
            offset = f.read_bytes(Int64)
            timestamp = f.read_bytes(Int64)

            count.should be > 0
            offset.should be >= 0
            timestamp.should be > 0
          end
        end

        store.close
      end
    end
  end

  describe "Metadata file reading on startup" do
    it "produces metadata files when missing on load" do
      with_datadir do |dir|
        store = LavinMQ::MessageStore.new(dir, nil)

        large_message = "x" * (LavinMQ::Config.instance.segment_size // 4) # message is 1/4 of segment size
        5.times { store.push(LavinMQ::Message.new("", "rk", large_message)) }

        # delete all meta files
        Dir.glob(File.join(dir, "meta.*")).each { |path| File.delete(path) }
        Dir.glob(File.join(dir, "meta.*")).should be_empty

        # Manually create a new message store to simulate restart behavior
        new_store = LavinMQ::MessageStore.new(dir, nil)

        # Should have loaded messages correctly
        new_store.size.should eq 5
        new_store.@segments.size.should be > 1

        # Should have produced metadata file for the segment
        new_store.@segments.each do |seg_id, mfile|
          next if seg_id == new_store.@segments.last_key # skip current writing segment
          File.exists?(mfile.path.sub("msgs.", "meta.")).should be_true
        end

        new_store.close
        store.close
      end
    end

    it "falls back to message scanning when meta file missing" do
      with_datadir do |dir|
        store = LavinMQ::MessageStore.new(dir, nil)

        25.times { |i| store.push(LavinMQ::Message.new("", "rk", "message #{i}")) }

        # Remove any meta files to simulate missing metadata
        Dir.glob(File.join(dir, "meta.*")).each { |path| File.delete(path) }

        # Create new store - should fall back to message scanning
        new_store = LavinMQ::MessageStore.new(dir, nil)
        new_store.size.should eq 25
        new_store.close
        store.close
      end
    end
  end

  describe "Segment deletion with metadata cleanup" do
    it "deletes meta file when segment is deleted" do
      with_datadir do |dir|
        store = LavinMQ::MessageStore.new(dir, nil)

        # Fill multiple segments
        segment_size = LavinMQ::Config.instance.segment_size
        message_size = 50
        messages_needed = (segment_size * 2 / message_size).to_i + 20

        messages_needed.times { |i| store.push(LavinMQ::Message.new("", "rk", "message #{i}")) }

        # Should have multiple segments now
        store.@segments.size.should be > 1

        # Get paths before deletion (only for completed segments that have meta files)
        existing_meta_paths = [] of String
        store.@segments.each do |seg_id, mfile|
          next if seg_id == store.@segments.last_key # skip current writing segment
          meta_path = mfile.path.sub("msgs.", "meta.")
          if File.exists?(meta_path)
            existing_meta_paths << meta_path
          end
        end

        existing_meta_paths.should_not be_empty

        # Delete store
        store.delete

        # Verify meta files are deleted
        existing_meta_paths.each { |path| File.exists?(path).should be_false }
      end
    end

    it "deletes meta file when purging queue" do
      with_datadir do |dir|
        store = LavinMQ::MessageStore.new(dir, nil)

        # Fill segments enough to create multiple segments
        segment_size = LavinMQ::Config.instance.segment_size
        message_size = 50
        messages_needed = (segment_size * 2 / message_size).to_i + 20

        messages_needed.times { |i| store.push(LavinMQ::Message.new("", "rk", "message #{i}")) }

        # Collect existing meta file paths
        existing_meta_paths = [] of String
        store.@segments.each do |seg_id, mfile|
          next if seg_id == store.@segments.last_key # skip current writing segment
          meta_path = mfile.path.sub("msgs.", "meta.")
          existing_meta_paths << meta_path if File.exists?(meta_path)
        end

        existing_meta_paths.should_not be_empty

        # Purge store
        store.purge

        # Verify meta files are deleted
        existing_meta_paths.each { |path| File.exists?(path).should be_false }

        store.close
      end
    end

    it "only completed segments have meta files" do
      with_datadir do |dir|
        store = LavinMQ::MessageStore.new(dir, nil)

        # Create enough messages to span multiple segments
        segment_size = LavinMQ::Config.instance.segment_size
        message_size = 50
        messages_needed = (segment_size * 2 / message_size).to_i + 20

        messages_needed.times { |i| store.push(LavinMQ::Message.new("", "rk", "message #{i}")) }

        initial_segments = store.@segments.size
        initial_segments.should be > 1

        # Verify only completed segments have meta files
        completed_segments = 0
        store.@segments.each do |seg_id, mfile|
          meta_path = mfile.path.sub("msgs.", "meta.")
          if seg_id == store.@segments.last_key
            # Current writing segment should not have meta file
            File.exists?(meta_path).should be_false
          else
            # Completed segments should have meta files
            if File.exists?(meta_path)
              completed_segments += 1
              # Verify the file has valid content
              count = File.open(meta_path, &.read_bytes(UInt32))
              count.should be > 0
            end
          end
        end

        completed_segments.should be > 0
        completed_segments.should eq initial_segments - 1

        store.close
      end
    end
  end

  describe "Count file functionality" do
    it "stores message count in metadata files" do
      with_datadir do |dir|
        store = LavinMQ::MessageStore.new(dir, nil)

        # Create enough messages to fill multiple segments
        segment_size = LavinMQ::Config.instance.segment_size
        message_size = 50
        messages_needed = (segment_size * 2 / message_size).to_i + 20

        messages_needed.times { |i| store.push(LavinMQ::Message.new("", "rk", "message #{i}")) }
        store.@segments.size.should be > 1

        # Verify meta files contain message counts for completed segments
        store.@segments.each do |seg_id, mfile|
          next if seg_id == store.@segments.last_key # skip current writing segment
          meta_path = mfile.path.sub("msgs.", "meta.")

          if File.exists?(meta_path)
            count = File.open(meta_path, &.read_bytes(UInt32))
            count.should eq store.@segment_msg_count[seg_id]
          else
            fail "Meta file #{meta_path} should exist"
          end
        end

        store.close
      end
    end

    it "uses metadata for faster message store initialization" do
      with_datadir do |dir|
        store = LavinMQ::MessageStore.new(dir, nil)

        # Publish messages
        large_message = "x" * (LavinMQ::Config.instance.segment_size // 4) # message is 1/4 of segment size
        message_count = 5
        message_count.times { store.push(LavinMQ::Message.new("", "rk", large_message)) }
        store.@segments.size.should be > 1
        Dir.glob(File.join(dir, "meta.*")).should_not be_empty

        new_store = LavinMQ::MessageStore.new(dir, nil)

        # Should have correct message count
        new_store.size.should eq message_count
        new_store.close
        store.close
      end
    end
  end
end

private def push_indexed_messages(store, count)
  body_size = LavinMQ::Config.instance.segment_size // 4 # 3 messages fit in a segment
  count.times do |i|
    store.push(LavinMQ::Message.new("", "rk", "#{i}:".ljust(body_size, 'x')))
  end
end

# The segment's size reached the disk but none of its data did: a power cut
# leaves it as zeros, which the loader takes as an empty segment
private def empty_segment(path : String) : Nil
  File.write(path, Bytes.new(File.size(path)))
end

private def body_at_offset(store, offset) : String
  _, seg, pos = store.find_offset(offset)
  String.new(store.read(seg, pos).should_not(be_nil).message.body)
end

# A segment that only got unconfirmed publishes isn't synced, so a power cut
# can bring it back empty, and the loader deletes it. The segments after it
# must still get offsets that continue from the segments before it.
describe "Stream segment lost in a power cut" do
  it "keeps the offsets of the next segment when an empty segment is deleted at load" do
    with_datadir do |dir|
      store = LavinMQ::AMQP::StreamMessageStore.new(dir, nil)
      push_indexed_messages(store, 12)
      store.@segments.size.should eq 4
      store.close

      empty_segment(File.join(dir, "msgs.0000000003"))
      store = LavinMQ::AMQP::StreamMessageStore.new(dir, nil)
      store.last_offset.should eq 12
      body_at_offset(store, 10).should start_with "9:"
      store.close
    end
  end

  it "continues the offsets after an empty segment when the meta files are lost too" do
    with_datadir do |dir|
      store = LavinMQ::AMQP::StreamMessageStore.new(dir, nil)
      push_indexed_messages(store, 12)
      store.@segments.size.should eq 4
      store.close

      empty_segment(File.join(dir, "msgs.0000000003"))
      Dir.glob(File.join(dir, "meta.*")).each { |path| File.write(path, "") }
      store = LavinMQ::AMQP::StreamMessageStore.new(dir, nil)
      store.last_offset.should eq 9
      body_at_offset(store, 7).should start_with "9:"
      store.close
    end
  end
end

# Mirrors Clustering::Server's registration contract: appending by position
# requires the path to be registered as an MFile first (clustering/server.cr)
private class StrictReplicator
  include LavinMQ::Clustering::Replicator

  getter mfile_paths = Set(String).new

  def register_file(path : String); end

  def register_file(file : File); end

  def register_file(mfile : MFile)
    @mfile_paths << mfile.path
  end

  def replace_file(path : String); end

  def replace_file(mfile : MFile); end

  def append(path : String, pos : Int, length : Int)
    raise ArgumentError.new("append(pos, length) requires an MFile-registered path: #{path}") unless @mfile_paths.includes?(path)
  end

  def append_value(path : String, value : UInt32 | Int32, offset : Int64); end

  def append_bytes(path : String, bytes : Bytes, offset : Int64); end

  def delete_file(path : String)
    @mfile_paths.delete(path)
  end

  def delete_dir(path : String); end

  def followers : Array(LavinMQ::Clustering::Follower)
    Array(LavinMQ::Clustering::Follower).new
  end

  def syncing_followers : Array(LavinMQ::Clustering::Follower)
    Array(LavinMQ::Clustering::Follower).new
  end

  def all_followers : Array(LavinMQ::Clustering::Follower)
    Array(LavinMQ::Clustering::Follower).new
  end

  def isr_dirty? : Bool
    false
  end

  def flush_isr : Nil; end

  def wait_for_followers : Nil; end

  def request_fsync(paths : Enumerable(String)) : Nil; end

  def request_syncfs : Nil; end

  def close; end

  def listen(server : TCPServer); end

  def clear; end

  def password : String
    ""
  end
end

private def damage_segment(dir, seg : Int, state : Symbol)
  path = File.join(dir, "msgs.#{seg.to_s.rjust(10, '0')}")
  case state
  when :zeroed then empty_segment(path)
  when :zeroed_no_meta
    empty_segment(path)
    File.delete?(File.join(dir, "meta.#{seg.to_s.rjust(10, '0')}"))
  end
end

# What recovery must produce: a zeroed segment with a readable meta knows
# where it ended; without one it continues from the chain, like a segment
# with data does.
private def expected_recovery(damage : Array(Symbol)) : Tuple(Hash(UInt32, Int64), Int64)
  firsts = Hash(UInt32, Int64).new
  prev_first = nil.as(Int64?)
  prev_count = 0i64
  damage.each_with_index do |state, i|
    seg = i + 1
    chain = prev_first.try { |pf| pf + prev_count }
    first, count = case state
                   when :zeroed
                     if seg < damage.size # a finished segment, its meta said where it ended
                       {3i64*seg + 1, 0i64}
                     else # the write segment never has a meta file
                       {chain || 1i64, 0i64}
                     end
                   when :zeroed_no_meta
                     # the next segment's meta file, when readable, says where this one ended
                     nxt_has_meta = seg + 1 < damage.size && damage[seg]? != :zeroed_no_meta
                     {nxt_has_meta ? 3i64*seg + 1 : (chain || 1i64), 0i64}
                   else # :ok
                     {chain || 1i64, 3i64}
                   end
    firsts[seg.to_u32] = first
    prev_first = first
    prev_count = count
  end
  {firsts, (prev_first || 1i64) + prev_count - 1}
end

private def assert_recovered(store, damage, firsts, last, file = __FILE__, line = __LINE__)
  store.last_offset.should eq(last), file: file, line: line
  store.@segment_first_offset.should eq(firsts), file: file, line: line
  damage.each_with_index do |state, i|
    next unless state == :ok
    seg = i + 1
    3.times do |j|
      body_at_offset(store, firsts[seg.to_u32] + j).should start_with("#{3*i + j}:"), file: file, line: line
    end
  end
end

describe "Stream power cut recovery invariants" do
  damage_states = [:ok, :zeroed, :zeroed_no_meta]
  damage_states.each do |s1|
    damage_states.each do |s2|
      damage_states.each do |s3|
        [:ok, :zeroed].each do |s4|
          damage = [s1, s2, s3, s4]
          it "recovers from damage #{damage.join(", ")}" do
            with_datadir do |dir|
              store = LavinMQ::AMQP::StreamMessageStore.new(dir, nil)
              push_indexed_messages(store, 12)
              store.@segments.size.should eq 4
              store.close
              damage.each_with_index { |state, i| damage_segment(dir, i + 1, state) }
              firsts, last = expected_recovery(damage)

              store = LavinMQ::AMQP::StreamMessageStore.new(dir, nil)
              assert_recovered(store, damage, firsts, last)
              store.close

              # recovery must be durable: a second restart reads the same numbers
              store = LavinMQ::AMQP::StreamMessageStore.new(dir, nil)
              assert_recovered(store, damage, firsts, last)

              # and the numbering continues, never reusing a known offset
              store.push(LavinMQ::Message.new("", "rk", "fresh"))
              store.last_offset.should eq last + 1
              body_at_offset(store, last + 1).should eq "fresh"
              store.close
            end
          end
        end
      end
    end
  end

  it "publishes after recovery on a clustered node" do
    with_datadir do |dir|
      store = LavinMQ::AMQP::StreamMessageStore.new(dir, nil)
      push_indexed_messages(store, 12)
      store.close

      empty_segment(File.join(dir, "msgs.0000000003"))
      empty_segment(File.join(dir, "msgs.0000000004"))
      replicator = StrictReplicator.new
      store = LavinMQ::AMQP::StreamMessageStore.new(dir, replicator)
      store.last_offset.should eq 9
      store.push(LavinMQ::Message.new("", "rk", "fresh")) # raises if the write segment lost its registration
      body_at_offset(store, 10).should eq "fresh"
      store.close
    end
  end

  it "rejects a torn meta file of a lost segment" do
    with_datadir do |dir|
      store = LavinMQ::AMQP::StreamMessageStore.new(dir, nil)
      push_indexed_messages(store, 12)
      store.close

      empty_segment(File.join(dir, "msgs.0000000003"))
      File.write(File.join(dir, "meta.0000000003"), Bytes.new(28, 0x78))
      store = LavinMQ::AMQP::StreamMessageStore.new(dir, nil)
      store.last_offset.should eq 9 # degraded like a lost meta file, never garbage offsets
      store.close
    end
  end

  it "corrects a stale stored offset in the meta after a lost segment" do
    with_datadir do |dir|
      store = LavinMQ::AMQP::StreamMessageStore.new(dir, nil)
      push_indexed_messages(store, 15)
      store.@segments.size.should eq 5
      store.close

      empty_segment(File.join(dir, "msgs.0000000003"))
      File.open(File.join(dir, "meta.0000000004"), "r+") do |f|
        f.seek(4) # the first offset field, after the message count
        f.write_bytes 99i64
      end
      store = LavinMQ::AMQP::StreamMessageStore.new(dir, nil)
      store.last_offset.should eq 15
      body_at_offset(store, 10).should start_with "9:"
      store.close
    end
  end

  it "syncs a finished segment and its meta file in the background" do
    with_datadir do |dir|
      persister = LavinMQ::Persister.new(dir)
      store = LavinMQ::AMQP::StreamMessageStore.new(dir, nil, persister: persister)
      push_indexed_messages(store, 4) # rolls over to segment 2
      store.@segments.size.should eq 2
      # the drain runs without any confirm, so confirms never pay for it
      wait_for { persister.last_sync }
      synced = persister.last_sync.should_not be_nil
      synced.paths.should contain File.join(dir, "msgs.0000000001")
      synced.paths.should contain File.join(dir, "meta.0000000001")
      store.close
      persister.close
    end
  end

  it "finds the next surviving message when the asked offset is lost" do
    with_datadir do |dir|
      store = LavinMQ::AMQP::StreamMessageStore.new(dir, nil)
      push_indexed_messages(store, 12)
      store.close

      empty_segment(File.join(dir, "msgs.0000000003"))
      store = LavinMQ::AMQP::StreamMessageStore.new(dir, nil)
      body_at_offset(store, 8).should start_with "9:" # offsets 7-9 are lost, 10 is next
      store.close
    end
  end

  it "gives a recovered segment its neighbors' timestamps, not the restart time" do
    with_datadir do |dir|
      store = LavinMQ::AMQP::StreamMessageStore.new(dir, nil)
      push_indexed_messages(store, 12)
      store.close

      empty_segment(File.join(dir, "msgs.0000000003"))
      File.delete(File.join(dir, "meta.0000000003")) # nothing says when it ended
      store = LavinMQ::AMQP::StreamMessageStore.new(dir, nil)
      # a restart-time stamp would break the time order lookups and max-age depend on
      store.@segment_first_ts[3u32].should be <= store.@segment_first_ts[4u32]
      store.@segment_last_ts[3u32].should be <= store.@segment_first_ts[4u32]
      store.close
    end
  end

  it "keeps the numbering when the recovered segment is lost again" do
    with_datadir do |dir|
      store = LavinMQ::AMQP::StreamMessageStore.new(dir, nil)
      push_indexed_messages(store, 12)
      store.close

      empty_segment(File.join(dir, "msgs.0000000003"))
      store = LavinMQ::AMQP::StreamMessageStore.new(dir, nil)
      store.last_offset.should eq 12
      store.close

      # a second power cut zeroes the recovered segment's header too
      empty_segment(File.join(dir, "msgs.0000000003"))
      store = LavinMQ::AMQP::StreamMessageStore.new(dir, nil)
      store.last_offset.should eq 12
      body_at_offset(store, 10).should start_with "9:"
      store.close
    end
  end

  it "counts publishes into a recovered last segment across restarts" do
    with_datadir do |dir|
      store = LavinMQ::AMQP::StreamMessageStore.new(dir, nil)
      push_indexed_messages(store, 12)
      store.close

      # the power cut hit right after a rollover: segment 4 never reached the
      # disk, segment 3 lost its data, but segment 3's meta file is durable
      File.delete(File.join(dir, "msgs.0000000004"))
      File.delete(File.join(dir, "meta.0000000004")) if File.exists?(File.join(dir, "meta.0000000004"))
      empty_segment(File.join(dir, "msgs.0000000003"))
      store = LavinMQ::AMQP::StreamMessageStore.new(dir, nil)
      store.last_offset.should eq 9
      store.close

      # a clean restart makes the recovered segment the write segment
      store = LavinMQ::AMQP::StreamMessageStore.new(dir, nil)
      store.last_offset.should eq 9
      store.push(LavinMQ::Message.new("", "rk", "a"))
      store.push(LavinMQ::Message.new("", "rk", "b"))
      store.last_offset.should eq 11
      store.close

      store = LavinMQ::AMQP::StreamMessageStore.new(dir, nil)
      store.last_offset.should eq 11
      body_at_offset(store, 10).should eq "a"
      body_at_offset(store, 11).should eq "b"
      store.close
    end
  end
end
