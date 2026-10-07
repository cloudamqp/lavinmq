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
