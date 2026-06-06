require "../spec_helper"

module SparkplugSpecs
  # Helper module to build protobuf payloads for testing
  module ProtobufBuilder
    # Encode a varint to bytes
    def self.encode_varint(value : UInt64) : Bytes
      io = IO::Memory.new
      loop do
        byte = (value & 0x7F).to_u8
        value >>= 7
        if value != 0
          byte |= 0x80
        end
        io.write_byte(byte)
        break if value == 0
      end
      io.to_slice
    end

    # Build a protobuf field tag
    def self.build_tag(field_number : UInt32, wire_type : UInt8) : Bytes
      encode_varint((field_number.to_u64 << 3) | wire_type.to_u64)
    end

    # Build a length-delimited field (string, bytes, or message)
    def self.build_length_delimited(field_number : UInt32, data : Bytes) : Bytes
      io = IO::Memory.new
      io.write(build_tag(field_number, 2_u8)) # Wire type 2 = length-delimited
      io.write(encode_varint(data.size.to_u64))
      io.write(data)
      io.to_slice
    end

    # Build a varint field
    def self.build_varint_field(field_number : UInt32, value : UInt64) : Bytes
      io = IO::Memory.new
      io.write(build_tag(field_number, 0_u8)) # Wire type 0 = varint
      io.write(encode_varint(value))
      io.to_slice
    end

    # Build a Sparkplug Metric message.
    # Field layout: name=1, alias=2, timestamp=3, datatype=4, value=10 (int_value).
    def self.build_metric(name : String?, datatype : UInt32 = 1_u32, with_value : Bool = true, alias_id : UInt64? = nil) : Bytes
      io = IO::Memory.new

      # Field 1: name (string, length-delimited) - omitted for alias-only metrics
      if name
        io.write(build_length_delimited(1_u32, name.to_slice))
      end

      # Field 2: alias (varint)
      if alias_id
        io.write(build_varint_field(2_u32, alias_id))
      end

      # Field 4: datatype (varint)
      io.write(build_varint_field(4_u32, datatype.to_u64))

      # Field 10: int_value (optional)
      if with_value
        io.write(build_varint_field(10_u32, 42_u64))
      end

      io.to_slice
    end

    # Build a complete Sparkplug Payload message.
    # bdSeq is encoded as a Metric named "bdSeq" (not a top-level field).
    def self.build_payload(
      timestamp : UInt64? = nil,
      metrics : Array(String)? = nil,
      seq : UInt64? = nil,
      bdseq : UInt64? = nil,
    ) : Bytes
      io = IO::Memory.new

      # Field 1: timestamp
      if timestamp
        io.write(build_varint_field(1_u32, timestamp))
      end

      # Field 2: metrics (repeated)
      if metrics
        metrics.each do |metric_name|
          io.write(build_length_delimited(2_u32, build_metric(metric_name)))
        end
      end

      # bdSeq carried as a metric named "bdSeq"
      if bdseq
        io.write(build_length_delimited(2_u32, build_metric("bdSeq", datatype: 8_u32)))
      end

      # Field 3: seq
      if seq
        io.write(build_varint_field(3_u32, seq))
      end

      io.to_slice
    end
  end

  describe LavinMQ::MQTT::Sparkplug::ProtobufValidator do
    describe "#validate_payload" do
      context "BIRTH messages (NBIRTH/DBIRTH)" do
        it "accepts valid NBIRTH payload with all required fields" do
          payload = ProtobufBuilder.build_payload(
            timestamp: 1234567890_u64,
            metrics: ["temperature", "pressure"],
            seq: 1_u64,
            bdseq: 0_u64
          )

          result = LavinMQ::MQTT::Sparkplug::ProtobufValidator.validate_payload(
            payload,
            LavinMQ::MQTT::Sparkplug::MessageType::NBIRTH
          )

          result.valid?.should be_true
          result.error.should be_nil
        end

        it "accepts valid DBIRTH payload with all required fields" do
          payload = ProtobufBuilder.build_payload(
            timestamp: 1234567890_u64,
            metrics: ["sensor1"],
            seq: 1_u64,
            bdseq: 0_u64
          )

          result = LavinMQ::MQTT::Sparkplug::ProtobufValidator.validate_payload(
            payload,
            LavinMQ::MQTT::Sparkplug::MessageType::DBIRTH
          )

          result.valid?.should be_true
        end

        it "rejects NBIRTH without timestamp" do
          payload = ProtobufBuilder.build_payload(
            metrics: ["temperature"],
            seq: 1_u64,
            bdseq: 0_u64
          )

          result = LavinMQ::MQTT::Sparkplug::ProtobufValidator.validate_payload(
            payload,
            LavinMQ::MQTT::Sparkplug::MessageType::NBIRTH
          )

          result.valid?.should be_false
          result.error.should_not be_nil
          result.error.not_nil!.should contain("timestamp")
        end

        it "accepts NBIRTH whose only metric is bdSeq" do
          # bdSeq is itself a metric, so an NBIRTH carrying just it is valid.
          payload = ProtobufBuilder.build_payload(
            timestamp: 1234567890_u64,
            seq: 1_u64,
            bdseq: 0_u64
          )

          result = LavinMQ::MQTT::Sparkplug::ProtobufValidator.validate_payload(
            payload,
            LavinMQ::MQTT::Sparkplug::MessageType::NBIRTH
          )

          result.valid?.should be_true
        end

        it "rejects DBIRTH without metrics" do
          payload = ProtobufBuilder.build_payload(
            timestamp: 1234567890_u64,
            seq: 1_u64
          )

          result = LavinMQ::MQTT::Sparkplug::ProtobufValidator.validate_payload(
            payload,
            LavinMQ::MQTT::Sparkplug::MessageType::DBIRTH
          )

          result.valid?.should be_false
          result.error.should_not be_nil
          result.error.not_nil!.should contain("metric")
        end

        it "rejects NBIRTH without seq" do
          payload = ProtobufBuilder.build_payload(
            timestamp: 1234567890_u64,
            metrics: ["temperature"],
            bdseq: 0_u64
          )

          result = LavinMQ::MQTT::Sparkplug::ProtobufValidator.validate_payload(
            payload,
            LavinMQ::MQTT::Sparkplug::MessageType::NBIRTH
          )

          result.valid?.should be_false
          result.error.should_not be_nil
          result.error.not_nil!.should contain("seq")
        end

        it "rejects NBIRTH without bdSeq" do
          payload = ProtobufBuilder.build_payload(
            timestamp: 1234567890_u64,
            metrics: ["temperature"],
            seq: 1_u64
          )

          result = LavinMQ::MQTT::Sparkplug::ProtobufValidator.validate_payload(
            payload,
            LavinMQ::MQTT::Sparkplug::MessageType::NBIRTH
          )

          result.valid?.should be_false
          result.error.should_not be_nil
          result.error.not_nil!.should contain("bdSeq")
        end
      end

      context "DATA messages (NDATA/DDATA)" do
        it "accepts valid NDATA payload with seq" do
          payload = ProtobufBuilder.build_payload(
            seq: 5_u64
          )

          result = LavinMQ::MQTT::Sparkplug::ProtobufValidator.validate_payload(
            payload,
            LavinMQ::MQTT::Sparkplug::MessageType::NDATA
          )

          result.valid?.should be_true
        end

        it "accepts valid DDATA payload with seq and metrics" do
          payload = ProtobufBuilder.build_payload(
            timestamp: 1234567890_u64,
            metrics: ["value1"],
            seq: 10_u64
          )

          result = LavinMQ::MQTT::Sparkplug::ProtobufValidator.validate_payload(
            payload,
            LavinMQ::MQTT::Sparkplug::MessageType::DDATA
          )

          result.valid?.should be_true
        end

        it "rejects NDATA without seq" do
          payload = ProtobufBuilder.build_payload(
            timestamp: 1234567890_u64,
            metrics: ["temperature"]
          )

          result = LavinMQ::MQTT::Sparkplug::ProtobufValidator.validate_payload(
            payload,
            LavinMQ::MQTT::Sparkplug::MessageType::NDATA
          )

          result.valid?.should be_false
          result.error.should_not be_nil
          result.error.not_nil!.should contain("seq")
        end
      end

      context "DEATH messages (NDEATH/DDEATH)" do
        it "accepts valid NDEATH payload with bdSeq" do
          payload = ProtobufBuilder.build_payload(
            bdseq: 0_u64
          )

          result = LavinMQ::MQTT::Sparkplug::ProtobufValidator.validate_payload(
            payload,
            LavinMQ::MQTT::Sparkplug::MessageType::NDEATH
          )

          result.valid?.should be_true
        end

        it "accepts valid DDEATH payload with seq" do
          # DDEATH carries a seq number (it is a device-level message), not bdSeq.
          payload = ProtobufBuilder.build_payload(
            timestamp: 1234567890_u64,
            seq: 7_u64
          )

          result = LavinMQ::MQTT::Sparkplug::ProtobufValidator.validate_payload(
            payload,
            LavinMQ::MQTT::Sparkplug::MessageType::DDEATH
          )

          result.valid?.should be_true
        end

        it "rejects DDEATH without seq" do
          payload = ProtobufBuilder.build_payload(
            timestamp: 1234567890_u64
          )

          result = LavinMQ::MQTT::Sparkplug::ProtobufValidator.validate_payload(
            payload,
            LavinMQ::MQTT::Sparkplug::MessageType::DDEATH
          )

          result.valid?.should be_false
          result.error.not_nil!.should contain("seq")
        end

        it "rejects NDEATH without bdSeq" do
          payload = ProtobufBuilder.build_payload(
            timestamp: 1234567890_u64
          )

          result = LavinMQ::MQTT::Sparkplug::ProtobufValidator.validate_payload(
            payload,
            LavinMQ::MQTT::Sparkplug::MessageType::NDEATH
          )

          result.valid?.should be_false
          result.error.should_not be_nil
          result.error.not_nil!.should contain("bdSeq")
        end

        it "accepts empty NDEATH payload" do
          payload = Bytes.empty

          result = LavinMQ::MQTT::Sparkplug::ProtobufValidator.validate_payload(
            payload,
            LavinMQ::MQTT::Sparkplug::MessageType::NDEATH
          )

          result.valid?.should be_true
        end
      end

      context "CMD messages (NCMD/DCMD)" do
        it "accepts valid NCMD payload" do
          payload = ProtobufBuilder.build_payload(
            timestamp: 1234567890_u64,
            metrics: ["command1"]
          )

          result = LavinMQ::MQTT::Sparkplug::ProtobufValidator.validate_payload(
            payload,
            LavinMQ::MQTT::Sparkplug::MessageType::NCMD
          )

          result.valid?.should be_true
        end

        it "accepts valid DCMD payload" do
          payload = ProtobufBuilder.build_payload(
            metrics: ["command1"],
            seq: 1_u64
          )

          result = LavinMQ::MQTT::Sparkplug::ProtobufValidator.validate_payload(
            payload,
            LavinMQ::MQTT::Sparkplug::MessageType::DCMD
          )

          result.valid?.should be_true
        end
      end

      context "invalid wire format" do
        it "rejects truncated varint" do
          # Create a payload with an incomplete varint (MSB set but no more bytes)
          io = IO::Memory.new
          io.write_byte(0xFF_u8) # Varint continuation byte with no following byte
          payload = io.to_slice

          result = LavinMQ::MQTT::Sparkplug::ProtobufValidator.validate_payload(
            payload,
            LavinMQ::MQTT::Sparkplug::MessageType::NDATA
          )

          result.valid?.should be_false
          result.error.should_not be_nil
          result.error.not_nil!.should contain("Invalid protobuf wire format")
        end

        it "rejects truncated length-delimited field" do
          # Create a payload with length-delimited field that claims more bytes than available
          io = IO::Memory.new
          io.write(ProtobufBuilder.build_tag(2_u32, 2_u8))
          io.write(ProtobufBuilder.encode_varint(100_u64)) # Claims 100 bytes
          io.write_byte(1_u8)                              # But only 1 byte follows
          payload = io.to_slice

          result = LavinMQ::MQTT::Sparkplug::ProtobufValidator.validate_payload(
            payload,
            LavinMQ::MQTT::Sparkplug::MessageType::NBIRTH
          )

          result.valid?.should be_false
          result.error.should_not be_nil
          result.error.not_nil!.should contain("Invalid protobuf wire format")
        end
      end

      context "metric validation" do
        it "rejects BIRTH metric without name field" do
          # An alias-only metric (no name) is invalid in a BIRTH message.
          metric_bytes = ProtobufBuilder.build_metric(nil, alias_id: 1_u64)

          payload_io = IO::Memory.new
          payload_io.write(ProtobufBuilder.build_varint_field(1_u32, 1234567890_u64))   # timestamp
          payload_io.write(ProtobufBuilder.build_length_delimited(2_u32, metric_bytes)) # invalid metric
          payload_io.write(ProtobufBuilder.build_varint_field(3_u32, 1_u64))            # seq
          payload = payload_io.to_slice

          result = LavinMQ::MQTT::Sparkplug::ProtobufValidator.validate_payload(
            payload,
            LavinMQ::MQTT::Sparkplug::MessageType::NBIRTH
          )

          result.valid?.should be_false
          result.error.not_nil!.should contain("name")
        end

        it "rejects BIRTH metric without datatype field" do
          # Build a metric with only name (field 1), no datatype (field 4)
          metric_io = IO::Memory.new
          metric_io.write(ProtobufBuilder.build_length_delimited(1_u32, "temperature".to_slice))
          metric_bytes = metric_io.to_slice

          payload_io = IO::Memory.new
          payload_io.write(ProtobufBuilder.build_varint_field(1_u32, 1234567890_u64))   # timestamp
          payload_io.write(ProtobufBuilder.build_length_delimited(2_u32, metric_bytes)) # invalid metric
          payload_io.write(ProtobufBuilder.build_varint_field(3_u32, 1_u64))            # seq
          payload = payload_io.to_slice

          result = LavinMQ::MQTT::Sparkplug::ProtobufValidator.validate_payload(
            payload,
            LavinMQ::MQTT::Sparkplug::MessageType::NBIRTH
          )

          result.valid?.should be_false
          result.error.not_nil!.should contain("datatype")
        end

        it "accepts alias-only metrics in DATA messages" do
          # NDATA/DDATA/NCMD/DCMD metrics reference an alias and omit the name.
          metric_bytes = ProtobufBuilder.build_metric(nil, alias_id: 7_u64)

          payload_io = IO::Memory.new
          payload_io.write(ProtobufBuilder.build_varint_field(1_u32, 1234567890_u64))   # timestamp
          payload_io.write(ProtobufBuilder.build_length_delimited(2_u32, metric_bytes)) # alias-only metric
          payload_io.write(ProtobufBuilder.build_varint_field(3_u32, 5_u64))            # seq
          payload = payload_io.to_slice

          result = LavinMQ::MQTT::Sparkplug::ProtobufValidator.validate_payload(
            payload,
            LavinMQ::MQTT::Sparkplug::MessageType::NDATA
          )

          result.valid?.should be_true
        end
      end

      context "edge cases" do
        it "accepts large varint values" do
          payload = ProtobufBuilder.build_payload(
            timestamp: UInt64::MAX,
            metrics: ["test"],
            seq: UInt64::MAX - 1,
            bdseq: UInt64::MAX - 2
          )

          result = LavinMQ::MQTT::Sparkplug::ProtobufValidator.validate_payload(
            payload,
            LavinMQ::MQTT::Sparkplug::MessageType::NBIRTH
          )

          result.valid?.should be_true
        end

        it "accepts payload with unknown fields (forward compatibility)" do
          # Build payload with known and unknown fields
          io = IO::Memory.new
          io.write(ProtobufBuilder.build_varint_field(1_u32, 1234567890_u64))                            # timestamp
          io.write(ProtobufBuilder.build_varint_field(99_u32, 42_u64))                                   # unknown field
          io.write(ProtobufBuilder.build_length_delimited(2_u32, ProtobufBuilder.build_metric("test")))  # metric
          io.write(ProtobufBuilder.build_length_delimited(2_u32, ProtobufBuilder.build_metric("bdSeq"))) # bdSeq metric
          io.write(ProtobufBuilder.build_varint_field(3_u32, 1_u64))                                     # seq
          payload = io.to_slice

          result = LavinMQ::MQTT::Sparkplug::ProtobufValidator.validate_payload(
            payload,
            LavinMQ::MQTT::Sparkplug::MessageType::NBIRTH
          )

          result.valid?.should be_true
        end
      end
    end
  end
end
