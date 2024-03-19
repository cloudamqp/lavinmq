require "spec"
require "../src/lavinmq/mfile"

module MFileSpec
  def self.with_file(&)
    file = File.tempfile "mfile_spec"
    yield file
  ensure
    file.delete unless file.nil?
  end

  describe MFile do
    describe "#write" do
      it "should raise ClosedError if closed" do
        with_file do |file|
          mfile = MFile.new file.path
          mfile.close
          expect_raises(MFile::ClosedError) { mfile.write "foo".to_slice }
        end
      end
    end
    describe "#read" do
      it "should raise ClosedError if closed" do
        with_file do |file|
          mfile = MFile.new file.path
          mfile.close
          data = Bytes.new(1)
          expect_raises(MFile::ClosedError) { mfile.read data }
        end
      end
    end
    describe "#to_slice" do
      describe "without position and size" do
        it "should raise ClosedError if closed" do
          with_file do |file|
            mfile = MFile.new file.path
            mfile.close
            expect_raises(MFile::ClosedError) { mfile.to_slice }
          end
        end
      end
      describe "with position and size" do
        it "should raise ClosedError if closed" do
          with_file do |file|
            mfile = MFile.new file.path
            mfile.close
            expect_raises(MFile::ClosedError) { mfile.to_slice(1, 1) }
          end
        end
      end
    end
    describe "#copy_to" do
      it "should raise ClosedError if closed" do
        with_file do |file|
          mfile = MFile.new file.path
          mfile.close
          data = IO::Memory.new
          expect_raises(MFile::ClosedError) { mfile.copy_to data }
        end
      end
    end
  end
end
