require "./spec_helper"

describe "Message segment metadata files" do
  describe "Metadata file creation" do
    it "creates meta file when segment becomes full" do
      with_amqp_server do |s|
        vhost = s.vhosts.create("test_vhost")
        with_channel(s, vhost: vhost.name) do |ch|
          q = ch.queue("meta_test")
          queue = vhost.queues["meta_test"].as(LavinMQ::AMQP::DurableQueue)

          # Create a large message to fill segments faster
          segment_size = LavinMQ::Config.instance.segment_size
          large_message = "x" * (segment_size // 4) # message is 1/4 of segment size

          # Publish enough messages to create multiple segments
          10.times do |i|
            q.publish_confirm "#{large_message}_#{i}"
          end
          queue.@msg_store.@segments.size.should be > 1

          # Check that meta files exist for completed segments
          meta_files = 0
          queue.@msg_store.@segments.each do |seg_id, mfile|
            next if seg_id == queue.@msg_store.@segments.last_key # skip current writing segment
            meta_path = mfile.path.sub("msgs.", "meta.")
            if File.exists?(meta_path)
              meta_files += 1
              # Verify meta file contains message count
              count = File.open(meta_path, &.read_bytes(UInt32))
              count.should eq queue.@msg_store.@segment_msg_count[seg_id]
            else
              fail "Meta file #{meta_path} should exist"
            end
          end
          meta_files.should eq queue.@msg_store.@segments.size - 1
        end
      end
    end

    it "writes correct metadata for stream queues" do
      with_amqp_server do |s|
        vhost = s.vhosts.create("test_vhost")
        with_channel(s, vhost: vhost.name) do |ch|
          q = ch.queue("stream_meta_test", args: AMQP::Client::Arguments.new({"x-queue-type" => "stream"}))
          queue = vhost.queues["stream_meta_test"].as(LavinMQ::AMQP::Stream)

          # Publish enough messages to trigger new segment creation
          segment_size = LavinMQ::Config.instance.segment_size
          message_size = 50
          messages_needed = (segment_size / message_size).to_i + 10

          messages_needed.times do |i|
            q.publish_confirm "message #{i}"
          end

          # Should have created new segments
          queue.@msg_store.@segments.size.should be > 1

          # Check meta file exists and contains stream-specific data
          queue.@msg_store.@segments.each do |seg_id, mfile|
            next if seg_id == queue.@msg_store.@segments.last_key # skip current writing segment
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
        end
      end
    end
  end

  describe "Metadata file reading on startup" do
    it "produces metadata files when missing on load" do
      with_amqp_server do |s|
        vhost = s.vhosts.create("test_vhost")
        with_channel(s, vhost: vhost.name) do |ch|
          q = ch.queue("metadata_load_test")
          queue = vhost.queues["metadata_load_test"].as(LavinMQ::AMQP::DurableQueue)

          large_message = "x" * (LavinMQ::Config.instance.segment_size // 4) # message is 1/4 of segment size
          5.times { q.publish_confirm large_message }

          # delete all meta files
          Dir.glob(File.join(queue.@msg_store.@msg_dir, "meta.*")).each { |path| File.delete(path) }
          Dir.glob(File.join(queue.@msg_store.@msg_dir, "meta.*")).should be_empty

          # Manually create a new message store to simulate restart behavior
          msg_dir = queue.@msg_store.@msg_dir
          new_store = LavinMQ::MessageStore.new(msg_dir, nil)

          # Should have loaded messages correctly
          new_store.size.should eq 5
          new_store.@segments.size.should be > 1

          # Should have produced metadata file for the segment
          new_store.@segments.each do |seg_id, mfile|
            next if seg_id == new_store.@segments.last_key # skip current writing segment
            File.exists?(mfile.path.sub("msgs.", "meta.")).should be_true
          end

          new_store.close
        end
      end
    end

    it "falls back to message scanning when meta file missing" do
      with_amqp_server do |s|
        vhost = s.vhosts.create("test_vhost")
        with_channel(s, vhost: vhost.name) do |ch|
          q = ch.queue("fallback_test")
          queue = vhost.queues["fallback_test"].as(LavinMQ::AMQP::DurableQueue)

          25.times { |i| q.publish_confirm "message #{i}" }
          msg_dir = queue.@msg_store.@msg_dir

          # Remove any meta files to simulate missing metadata
          Dir.glob(File.join(msg_dir, "meta.*")).each { |path| File.delete(path) }

          # Create new store - should fall back to message scanning
          new_store = LavinMQ::MessageStore.new(msg_dir, nil)
          new_store.size.should eq 25
          new_store.close
        end
      end
    end
  end

  describe "Segment deletion with metadata cleanup" do
    it "deletes meta file when segment is deleted" do
      with_amqp_server do |s|
        vhost = s.vhosts.create("test_vhost")
        with_channel(s, vhost: vhost.name) do |ch|
          q = ch.queue("delete_test")
          queue = vhost.queues["delete_test"].as(LavinMQ::AMQP::DurableQueue)

          # Fill multiple segments
          segment_size = LavinMQ::Config.instance.segment_size
          message_size = 50
          messages_needed = (segment_size * 2 / message_size).to_i + 20

          messages_needed.times { |i| q.publish_confirm "message #{i}" }

          # Should have multiple segments now
          queue.@msg_store.@segments.size.should be > 1

          # Get paths before deletion (only for completed segments that have meta files)
          existing_meta_paths = [] of String
          queue.@msg_store.@segments.each do |seg_id, mfile|
            next if seg_id == queue.@msg_store.@segments.last_key # skip current writing segment
            meta_path = mfile.path.sub("msgs.", "meta.")
            if File.exists?(meta_path)
              existing_meta_paths << meta_path
            end
          end

          existing_meta_paths.should_not be_empty

          # Delete queue
          queue.delete

          # Verify meta files are deleted
          existing_meta_paths.each { |path| File.exists?(path).should be_false }
        end
      end
    end

    it "deletes meta file when purging queue" do
      with_amqp_server do |s|
        vhost = s.vhosts.create("test_vhost")
        with_channel(s, vhost: vhost.name) do |ch|
          q = ch.queue("purge_test")
          queue = vhost.queues["purge_test"].as(LavinMQ::AMQP::DurableQueue)

          # Fill segments enough to create multiple segments
          segment_size = LavinMQ::Config.instance.segment_size
          message_size = 50
          messages_needed = (segment_size * 2 / message_size).to_i + 20

          messages_needed.times { |i| q.publish_confirm "message #{i}" }

          # Collect existing meta file paths
          existing_meta_paths = [] of String
          queue.@msg_store.@segments.each do |seg_id, mfile|
            next if seg_id == queue.@msg_store.@segments.last_key # skip current writing segment
            meta_path = mfile.path.sub("msgs.", "meta.")
            existing_meta_paths << meta_path if File.exists?(meta_path)
          end

          existing_meta_paths.should_not be_empty

          # Purge queue
          q.purge

          # Verify meta files are deleted
          existing_meta_paths.each { |path| File.exists?(path).should be_false }
        end
      end
    end

    it "only completed segments have meta files" do
      with_amqp_server do |s|
        vhost = s.vhosts.create("test_vhost")
        with_channel(s, vhost: vhost.name) do |ch|
          q = ch.queue("cleanup_test")
          queue = vhost.queues["cleanup_test"].as(LavinMQ::AMQP::DurableQueue)

          # Create enough messages to span multiple segments
          segment_size = LavinMQ::Config.instance.segment_size
          message_size = 50
          messages_needed = (segment_size * 2 / message_size).to_i + 20

          messages_needed.times { |i| q.publish_confirm "message #{i}" }

          initial_segments = queue.@msg_store.@segments.size
          initial_segments.should be > 1

          # Verify only completed segments have meta files
          completed_segments = 0
          queue.@msg_store.@segments.each do |seg_id, mfile|
            meta_path = mfile.path.sub("msgs.", "meta.")
            if seg_id == queue.@msg_store.@segments.last_key
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
        end
      end
    end
  end

  describe "Count file functionality" do
    it "stores message count in metadata files" do
      with_amqp_server do |s|
        vhost = s.vhosts.create("test_vhost")
        with_channel(s, vhost: vhost.name) do |ch|
          q = ch.queue("count_test")
          queue = vhost.queues["count_test"].as(LavinMQ::AMQP::DurableQueue)

          # Create enough messages to fill multiple segments
          segment_size = LavinMQ::Config.instance.segment_size
          message_size = 50
          messages_needed = (segment_size * 2 / message_size).to_i + 20

          messages_needed.times { |i| q.publish_confirm "message #{i}" }
          queue.@msg_store.@segments.size.should be > 1

          # Verify meta files contain message counts for completed segments
          queue.@msg_store.@segments.each do |seg_id, mfile|
            next if seg_id == queue.@msg_store.@segments.last_key # skip current writing segment
            meta_path = mfile.path.sub("msgs.", "meta.")

            if File.exists?(meta_path)
              count = File.open(meta_path, &.read_bytes(UInt32))
              count.should eq queue.@msg_store.@segment_msg_count[seg_id]
            else
              fail "Meta file #{meta_path} should exist"
            end
          end
        end
      end
    end

    it "uses metadata for faster message store initialization" do
      with_amqp_server do |s|
        vhost = s.vhosts.create("test_vhost")
        with_channel(s, vhost: vhost.name) do |ch|
          q = ch.queue("init_test")
          queue = vhost.queues["init_test"].as(LavinMQ::AMQP::DurableQueue)

          # Publish messages
          large_message = "x" * (LavinMQ::Config.instance.segment_size // 4) # message is 1/4 of segment size
          message_count = 5
          message_count.times { q.publish_confirm large_message }
          queue.@msg_store.@segments.size.should be > 1
          Dir.glob(File.join(queue.@msg_store.@msg_dir, "meta.*")).should_not be_empty

          msg_dir = queue.@msg_store.@msg_dir
          new_store = LavinMQ::MessageStore.new(msg_dir, nil)

          # Should have correct message count
          new_store.size.should eq message_count
          new_store.close
        end
      end
    end
  end
end
