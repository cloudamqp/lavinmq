require "../spec_helper"

def read_and_verify_filename(io, expected_filename)
  filename_size = io.read_bytes(Int32, IO::ByteFormat::LittleEndian)
  filename = Bytes.new(filename_size)
  io.read filename
  filename = String.new(filename)
  filename.should eq expected_filename
end

def read_and_verify_data(io, expected_data)
  data_size = io.read_bytes(Int64, IO::ByteFormat::LittleEndian)
  case expected_data
  when Int32
    io.read_bytes(Int32, IO::ByteFormat::LittleEndian).should eq expected_data
  when UInt32
    io.read_bytes(Int32, IO::ByteFormat::LittleEndian).should eq expected_data
  when String
    data = Bytes.new(data_size)
    io.read(data)
    String.new(data).should eq expected_data
  when Bytes
    data = Bytes.new(-data_size)
    io.read(data)
    data.should eq expected_data
  else
    raise "Unknown data type"
  end
end

describe LavinMQ::Clustering::Action do
  describe "AddAction" do
    describe "without @mfile" do
      describe "#send" do
        it "writes filename and data to IO" do
          with_datadir do |data_dir|
            filename = "file1"
            File.write File.join(data_dir, filename), "foo"
            action = LavinMQ::Clustering::AddAction.new data_dir, filename
            io = IO::Memory.new
            action.send io
            io.rewind

            read_and_verify_filename(io, filename)
            read_and_verify_data(io, "foo")
          end
        end
      end

      describe "#lag_size" do
        it "should count filename and filesize" do
          with_datadir do |data_dir|
            filename = "file1"
            File.write File.join(data_dir, filename), "foo"
            action = LavinMQ::Clustering::AddAction.new data_dir, filename
            action.lag_size.should eq(sizeof(Int32) + filename.bytesize + sizeof(Int64) + "foo".bytesize)
          end
        end
      end
    end

    describe "with @mfile" do
      describe "#send" do
        it "writes filename and data to IO" do
          with_datadir do |data_dir|
            filename = "file1"
            absolute = File.join data_dir, filename
            File.write absolute, "foo"
            action = LavinMQ::Clustering::AddAction.new data_dir, filename, MFile.new(absolute)
            io = IO::Memory.new
            action.send io
            io.rewind

            read_and_verify_filename(io, filename)
            read_and_verify_data(io, "foo")
          end
        end
      end
      describe "#lag_size" do
        it "should count filename and filesize" do
          with_datadir do |data_dir|
            filename = "file1"
            File.write File.join(data_dir, filename), "foo"
            action = LavinMQ::Clustering::AddAction.new data_dir, filename
            action.lag_size.should eq(sizeof(Int32) + filename.bytesize + sizeof(Int64) + "foo".bytesize)
          end
        end
      end
    end
  end

  describe "AppendAction" do
    describe "with Int32" do
      describe "#send" do
        it "writes filename and data to IO" do
          filename = "file1"
          data_dir = "/not/used"
          action = LavinMQ::Clustering::AppendAction.new data_dir, filename, 123i32
          io = IO::Memory.new
          action.send io
          io.rewind

          read_and_verify_filename(io, filename)
          read_and_verify_data(io, 123i32)
        end
      end
      describe "#lag_size" do
        it "should count filename and size of Int32" do
          filename = "file1"
          data_dir = "/not/used"
          action = LavinMQ::Clustering::AppendAction.new data_dir, filename, 123i32
          action.lag_size.should eq(sizeof(Int32) + filename.bytesize + sizeof(Int64) + sizeof(Int32))
        end
      end
    end

    describe "with UInt32" do
      describe "#send" do
        it "writes filename and data to IO" do
          filename = "file1"
          data_dir = "/not/used"
          action = LavinMQ::Clustering::AppendAction.new data_dir, filename, 123u32
          io = IO::Memory.new
          action.send io
          io.rewind

          read_and_verify_filename(io, filename)
          read_and_verify_data(io, 123u32)
        end
      end
      describe "#lag_size" do
        it "should count filename and size of UInt32" do
          data_dir = "/not/used"
          filename = "file1"
          action = LavinMQ::Clustering::AppendAction.new data_dir, filename, 123u32
          action.lag_size.should eq(sizeof(Int32) + filename.bytesize + sizeof(Int64) + sizeof(UInt32))
        end
      end
    end

    describe "with Bytes" do
      describe "#send" do
        it "writes filename and data to IO" do
          data = "foo"
          filename = "bar"
          data_dir = "/not/used"

          action = LavinMQ::Clustering::AppendAction.new data_dir, filename, data.to_slice
          io = IO::Memory.new
          action.send io
          io.rewind

          read_and_verify_filename(io, filename)
          read_and_verify_data(io, data.to_slice)
        end
      end
      describe "#lag_size" do
        it "should count filename and size of Bytes" do
          filename = "file1"
          data_dir = "/not/used"
          action = LavinMQ::Clustering::AppendAction.new data_dir, filename, "foo".to_slice
          action.lag_size.should eq(sizeof(Int32) + filename.bytesize + sizeof(Int64) + "foo".to_slice.bytesize)
        end
      end
    end

    describe "with FileRange" do
      describe "#send" do
        it "writes filename and data to IO" do
          with_datadir do |data_dir|
            filename = "file1"
            absolute = File.join data_dir, filename
            File.write absolute, "baz foo bar"
            range = LavinMQ::Clustering::FileRange.new(MFile.new(absolute), 4, 3)
            action = LavinMQ::Clustering::AppendAction.new data_dir, filename, range

            io = IO::Memory.new
            action.send io
            io.rewind

            read_and_verify_filename(io, filename)
            read_and_verify_data(io, "foo".to_slice)
          end
        end
      end
      describe "#lag_size" do
        it "should count filename and size of FileRange" do
          with_datadir do |data_dir|
            filename = "file1"
            absolute = File.join data_dir, filename
            File.write absolute, "foo bar baz"
            range = LavinMQ::Clustering::FileRange.new(MFile.new(absolute), 4, 3)
            action = LavinMQ::Clustering::AppendAction.new data_dir, filename, range
            action.lag_size.should eq(sizeof(Int32) + filename.bytesize + sizeof(Int64) + range.len)
          end
        end
      end
    end
  end
end
