require "./spec_helper"
require "../src/lavinmq/schema"

describe LavinMQ::SchemaVersion do
  describe "Schema version" do
    it "Empty file should raise EmptyFile" do
      data_dir = "/tmp/lavinmq-spec"
      path = File.join(data_dir, "test_schema_version")
      file = MFile.new(path, 12)
      expect_raises(LavinMQ::EmptyFile) do
        LavinMQ::SchemaVersion.verify(file, :message)
      end
    end

    it "Should verify schema version" do
      data_dir = "/tmp/lavinmq-spec"
      path = File.join(data_dir, "test_schema_version")
      file = MFile.new(path, 12)
      file.write_bytes LavinMQ::Schema::VERSION
      LavinMQ::SchemaVersion.verify(file, :message).should eq LavinMQ::SchemaVersion::VERSIONS[:message]
    end
  end
end
