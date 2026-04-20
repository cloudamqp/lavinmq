require "../spec_helper"
require "lz4"

module ClientSyncSpec
  class TestClient < LavinMQ::Clustering::Client
    def sync_files_public(socket, lz4)
      sync_files(socket, lz4)
    end
  end

  def self.make_client(data_dir : String) : TestClient
    config = LavinMQ::Config.instance.dup
    config.data_dir = data_dir
    config.metrics_http_port = -1
    TestClient.new(config, 1, "password", proxy: false)
  end

  def self.simulate_leader(io : IO, leader_files : Hash(String, String))
    lz4 = Compress::LZ4::Writer.new(io, Compress::LZ4::CompressOptions.new(auto_flush: true, block_mode_linked: true))
    leader_files.each do |filename, content|
      hash = Digest::SHA1.digest(content)
      lz4.write_bytes filename.bytesize, IO::ByteFormat::LittleEndian
      lz4.write filename.to_slice
      lz4.write hash
    end
    lz4.write_bytes 0i32, IO::ByteFormat::LittleEndian
    lz4.flush

    requested = Array(String).new
    loop do
      len = io.read_bytes Int32, IO::ByteFormat::LittleEndian
      break if len == 0
      filename = io.read_string(len)
      requested << filename
    end

    requested.each do |filename|
      content = leader_files[filename]? || ""
      lz4.write_bytes content.bytesize.to_i64, IO::ByteFormat::LittleEndian
      lz4.write content.to_slice
      lz4.flush
    end
  end

  describe LavinMQ::Clustering::Client do
    describe "sync_files directory cleanup" do
      it "deletes directory not on leader" do
        with_datadir do |data_dir|
          Dir.mkdir_p File.join(data_dir, "queue1")
          File.write File.join(data_dir, "queue1", "messages.dat"), "data"

          client = make_client(data_dir)
          server_io, client_io = UNIXSocket.pair
          lz4_reader = Compress::LZ4::Reader.new(client_io)

          done = Channel(Nil).new
          spawn do
            simulate_leader(server_io, {} of String => String)
            done.send nil
          end

          client.sync_files_public(client_io, lz4_reader)

          select
          when done.receive
          when timeout(1.second)
            fail "leader fiber timed out"
          end

          Dir.exists?(File.join(data_dir, "queue1")).should be_false
        end
      end

      it "deletes nested directory tree absent from leader" do
        with_datadir do |data_dir|
          Dir.mkdir_p File.join(data_dir, "a", "b", "c")
          File.write File.join(data_dir, "a", "b", "c", "file.dat"), "data"

          client = make_client(data_dir)
          server_io, client_io = UNIXSocket.pair
          lz4_reader = Compress::LZ4::Reader.new(client_io)

          done = Channel(Nil).new
          spawn do
            simulate_leader(server_io, {} of String => String)
            done.send nil
          end

          client.sync_files_public(client_io, lz4_reader)

          select
          when done.receive
          when timeout(1.second)
            fail "leader fiber timed out"
          end

          Dir.exists?(File.join(data_dir, "a", "b", "c")).should be_false
          Dir.exists?(File.join(data_dir, "a", "b")).should be_false
          Dir.exists?(File.join(data_dir, "a")).should be_false
        end
      end

      it "keeps directories containing files present on the leader" do
        with_datadir do |data_dir|
          content = "hello"
          Dir.mkdir_p File.join(data_dir, "queue1")
          File.write File.join(data_dir, "queue1", "messages.dat"), content

          client = make_client(data_dir)
          server_io, client_io = UNIXSocket.pair
          lz4_reader = Compress::LZ4::Reader.new(client_io)

          done = Channel(Nil).new
          spawn do
            simulate_leader(server_io, {"queue1/messages.dat" => content})
            done.send nil
          end

          client.sync_files_public(client_io, lz4_reader)

          select
          when done.receive
          when timeout(1.second)
            fail "leader fiber timed out"
          end

          Dir.exists?(File.join(data_dir, "queue1")).should be_true
          File.exists?(File.join(data_dir, "queue1", "messages.dat")).should be_true
        end
      end

      it "deletes only directories absent from leader" do
        with_datadir do |data_dir|
          content = "hello"
          Dir.mkdir_p File.join(data_dir, "queue1")
          Dir.mkdir_p File.join(data_dir, "queue2")
          File.write File.join(data_dir, "queue1", "messages.dat"), content
          File.write File.join(data_dir, "queue2", "messages.dat"), content

          client = make_client(data_dir)
          server_io, client_io = UNIXSocket.pair
          lz4_reader = Compress::LZ4::Reader.new(client_io)

          done = Channel(Nil).new
          spawn do
            simulate_leader(server_io, {"queue1/messages.dat" => content})
            done.send nil
          end

          client.sync_files_public(client_io, lz4_reader)

          select
          when done.receive
          when timeout(1.second)
            fail "leader fiber timed out"
          end

          Dir.exists?(File.join(data_dir, "queue1")).should be_true
          Dir.exists?(File.join(data_dir, "queue2")).should be_false
        end
      end
    end
  end
end
