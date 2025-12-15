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

    # Build a Sparkplug Metric message
    def self.build_metric(name : String, datatype : UInt32 = 1_u32, with_value : Bool = true) : Bytes
      io = IO::Memory.new

      # Field 1: name (string, length-delimited)
      name_bytes = name.to_slice
      io.write(build_length_delimited(1_u32, name_bytes))

      # Field 3: datatype (varint)
      io.write(build_varint_field(3_u32, datatype.to_u64))

      # Field 4-16: value (optional, we'll add a simple int value)
      if with_value
        io.write(build_varint_field(4_u32, 42_u64)) # int_value = 42
      end

      io.to_slice
    end

    # Build a complete Sparkplug Payload message
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
          metric_data = build_metric(metric_name)
          io.write(build_length_delimited(2_u32, metric_data))
        end
      end

      # Field 3: seq
      if seq
        io.write(build_varint_field(3_u32, seq))
      end

      # Field 4: bdSeq
      if bdseq
        io.write(build_varint_field(4_u32, bdseq))
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

        it "rejects NBIRTH without metrics" do
          payload = ProtobufBuilder.build_payload(
            timestamp: 1234567890_u64,
            seq: 1_u64,
            bdseq: 0_u64
          )

          result = LavinMQ::MQTT::Sparkplug::ProtobufValidator.validate_payload(
            payload,
            LavinMQ::MQTT::Sparkplug::MessageType::NBIRTH
          )

          result.valid?.should be_false
          result.error.should_not be_nil
          result.error.not_nil!.should contain("metrics")
        end

        it "rejects NBIRTH with empty metrics array" do
          payload = ProtobufBuilder.build_payload(
            timestamp: 1234567890_u64,
            metrics: [] of String,
            seq: 1_u64,
            bdseq: 0_u64
          )

          result = LavinMQ::MQTT::Sparkplug::ProtobufValidator.validate_payload(
            payload,
            LavinMQ::MQTT::Sparkplug::MessageType::NBIRTH
          )

          result.valid?.should be_false
          result.error.should_not be_nil
          result.error.not_nil!.should contain("metrics")
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

        it "accepts valid DDEATH payload with bdSeq" do
          payload = ProtobufBuilder.build_payload(
            timestamp: 1234567890_u64,
            bdseq: 1_u64
          )

          result = LavinMQ::MQTT::Sparkplug::ProtobufValidator.validate_payload(
            payload,
            LavinMQ::MQTT::Sparkplug::MessageType::DDEATH
          )

          result.valid?.should be_true
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
        it "rejects metric without name field" do
          # Build a metric with only datatype, no name
          io = IO::Memory.new
          io.write(ProtobufBuilder.build_varint_field(3_u32, 1_u64)) # datatype only
          metric_bytes = io.to_slice

          # Build payload with this invalid metric
          payload_io = IO::Memory.new
          payload_io.write(ProtobufBuilder.build_varint_field(1_u32, 1234567890_u64))   # timestamp
          payload_io.write(ProtobufBuilder.build_length_delimited(2_u32, metric_bytes)) # invalid metric
          payload_io.write(ProtobufBuilder.build_varint_field(3_u32, 1_u64))            # seq
          payload_io.write(ProtobufBuilder.build_varint_field(4_u32, 0_u64))            # bdSeq
          payload = payload_io.to_slice

          result = LavinMQ::MQTT::Sparkplug::ProtobufValidator.validate_payload(
            payload,
            LavinMQ::MQTT::Sparkplug::MessageType::NBIRTH
          )

          result.valid?.should be_false
          result.error.should_not be_nil
          result.error.not_nil!.should contain("name")
        end

        it "rejects metric without datatype field" do
          # Build a metric with only name, no datatype
          name_bytes = "temperature".to_slice
          metric_io = IO::Memory.new
          metric_io.write(ProtobufBuilder.build_length_delimited(1_u32, name_bytes)) # name only
          metric_bytes = metric_io.to_slice

          # Build payload with this invalid metric
          payload_io = IO::Memory.new
          payload_io.write(ProtobufBuilder.build_varint_field(1_u32, 1234567890_u64))   # timestamp
          payload_io.write(ProtobufBuilder.build_length_delimited(2_u32, metric_bytes)) # invalid metric
          payload_io.write(ProtobufBuilder.build_varint_field(3_u32, 1_u64))            # seq
          payload_io.write(ProtobufBuilder.build_varint_field(4_u32, 0_u64))            # bdSeq
          payload = payload_io.to_slice

          result = LavinMQ::MQTT::Sparkplug::ProtobufValidator.validate_payload(
            payload,
            LavinMQ::MQTT::Sparkplug::MessageType::NBIRTH
          )

          result.valid?.should be_false
          result.error.should_not be_nil
          result.error.not_nil!.should contain("datatype")
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
          io.write(ProtobufBuilder.build_varint_field(1_u32, 1234567890_u64)) # timestamp
          io.write(ProtobufBuilder.build_varint_field(99_u32, 42_u64))        # unknown field
          metric_bytes = ProtobufBuilder.build_metric("test")
          io.write(ProtobufBuilder.build_length_delimited(2_u32, metric_bytes)) # metrics
          io.write(ProtobufBuilder.build_varint_field(3_u32, 1_u64))            # seq
          io.write(ProtobufBuilder.build_varint_field(4_u32, 0_u64))            # bdSeq
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
