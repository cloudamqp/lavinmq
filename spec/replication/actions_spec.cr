require "../spec_helper"

def with_datadir_tempfile(&)
  relative_path = Path.new "data.spec"
  absolute_path = Path.new(LavinMQ::Config.instance.data_dir).join relative_path
  yield relative_path.to_s, absolute_path.to_s
ensure
  if absolute_path && File.exists? absolute_path
    FileUtils.rm absolute_path
  end
end

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

describe LavinMQ::Replication::Action do
  describe "AddAction" do
    describe "without @mfile" do
      describe "#send" do
        it "writes filename and data to IO" do
          with_datadir_tempfile do |relative, absolute|
            File.write absolute, "foo"
            action = LavinMQ::Replication::AddAction.new absolute
            io = IO::Memory.new
            action.send io
            io.rewind

            read_and_verify_filename(io, relative)
            read_and_verify_data(io, "foo")
          end
        end
      end
    end

    describe "with @mfile" do
      describe "#send" do
        it "writes filename and data to IO" do
          with_datadir_tempfile do |relative, absolute|
            File.write absolute, "foo"
            action = LavinMQ::Replication::AddAction.new absolute, MFile.new(absolute)
            io = IO::Memory.new
            action.send io
            io.rewind

            read_and_verify_filename(io, relative)
            read_and_verify_data(io, "foo")
          end
        end
      end
    end
  end

  describe "AppendAction" do
    describe "with Int32" do
      describe "#send" do
        it "writes filename and data to IO" do
          with_datadir_tempfile do |relative, absolute|
            action = LavinMQ::Replication::AppendAction.new absolute, 123i32
            io = IO::Memory.new
            action.send io
            io.rewind

            read_and_verify_filename(io, relative)
            read_and_verify_data(io, 123i32)
          end
        end
      end
    end

    describe "with Int32" do
      describe "#send" do
        it "writes filename and data to IO" do
          with_datadir_tempfile do |relative, absolute|
            action = LavinMQ::Replication::AppendAction.new absolute, 123u32
            io = IO::Memory.new
            action.send io
            io.rewind

            read_and_verify_filename(io, relative)
            read_and_verify_data(io, 123u32)
          end
        end
      end
    end

    describe "with Bytes" do
      describe "#send" do
        it "writes filename and data to IO" do
          with_datadir_tempfile do |relative, absolute|
            action = LavinMQ::Replication::AppendAction.new absolute, "foo".to_slice
            io = IO::Memory.new
            action.send io
            io.rewind

            read_and_verify_filename(io, relative)
            read_and_verify_data(io, "foo".to_slice)
          end
        end
      end
    end

    describe "with FileRange" do
      describe "#send" do
        it "writes filename and data to IO" do
          with_datadir_tempfile do |relative, absolute|
            File.write absolute, "baz foo bar"
            range = LavinMQ::Replication::FileRange.new(MFile.new(absolute), 4, 3)
            action = LavinMQ::Replication::AppendAction.new absolute, range

            io = IO::Memory.new
            action.send io
            io.rewind

            read_and_verify_filename(io, relative)
            read_and_verify_data(io, "foo".to_slice)
          end
        end
      end
    end
  end
end
