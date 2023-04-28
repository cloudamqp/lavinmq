require "./spec_helper"

describe LavinMQ::Logging::EntityLog do
  describe "#extend" do
    describe "with no arguments" do
      it "should return a new instance with same log and equal metadata" do
        log = ::Log.for "spec"
        initial = LavinMQ::Logging::EntityLog.new log, meta: "data"

        copy = initial.extend

        copy.log.should be initial.log
        # Same values, not same instance:
        copy.metadata.should_not be initial.metadata
        copy.metadata.should eq initial.metadata
      end
    end

    describe "with only log argument" do
      it "should return a new instance with given logger and equal metadata" do
        log = ::Log.for "spec"
        initial = LavinMQ::Logging::EntityLog.new log, meta: "data"

        other_log = ::Log.for("other")
        copy = initial.extend other_log

        copy.log.should be other_log
        # Same values, not same instance:
        copy.metadata.should_not be initial.metadata
        copy.metadata.should eq initial.metadata
      end
    end

    describe "with only metadata argument" do
      it "should return a new instance with same logger and metadata appended to existing" do
        log = ::Log.for "spec"
        initial = LavinMQ::Logging::EntityLog.new log, meta: "data"

        expected_metadata = ::Log::Metadata.build({meta: "data", foo: "bar"})

        copy = initial.extend foo: "bar"

        copy.log.should be initial.log
        copy.metadata.should eq expected_metadata
        # Check order of items
        copied_metadata_keys = copy.metadata.to_h.keys
        {:meta, :foo}.each_with_index do |key, i|
          copied_metadata_keys[i].should eq key
        end
      end
    end
  end
end
