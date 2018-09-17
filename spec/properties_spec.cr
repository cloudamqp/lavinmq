require "./spec_helper"

describe AvalancheMQ::AMQP::Table do
  it "can be encoded and decoded" do
    data = Hash(String, AvalancheMQ::AMQP::Field){
      "bool"    => true,
      "int8"    => Int8::MAX,
      "uint8"   => UInt8::MAX,
      "int16"   => Int16::MAX,
      "uint16"  => UInt16::MAX,
      "int32"   => Int32::MAX,
      "uint32"  => UInt32::MAX,
      "int64"   => Int64::MAX,
      "float32" => 0.0_f32,
      # "float64" => 0.0_f64,
      "string" => "a" * 257,
      "array"  => [
        true,
        Int8::MAX,
        UInt8::MAX,
        Int16::MAX,
        UInt16::MAX,
        Int32::MAX,
        UInt32::MAX,
        Int64::MAX,
        0.0_f32,
        # 0.0_f64,
        "a" * 257,
        "aaaa".to_slice,
        Time.epoch(Time.utc_now.epoch),
        Hash(String, AvalancheMQ::AMQP::Field){"key" => "value"},
        nil,
      ] of AvalancheMQ::AMQP::Field,
      "byte_array" => "aaaa".to_slice,
      "time"       => Time.epoch(Time.utc_now.epoch),
      "hash"       => Hash(String, AvalancheMQ::AMQP::Field){"key" => "value"},
      "nil"        => nil,
    }
    tbl = AvalancheMQ::AMQP::Table.new(data)
    io = IO::Memory.new
    io.write_bytes tbl, IO::ByteFormat::NetworkEndian
    io.pos.should eq(tbl.bytesize)
    io.pos = 0
    data2 = AvalancheMQ::AMQP::Table.from_io(io, IO::ByteFormat::NetworkEndian)
    data2.should eq data
  end
end

describe AvalancheMQ::AMQP::Properties do
  it "can be encoded and decoded" do
    io = AvalancheMQ::AMQP::MemoryIO.new
    h = Hash(String, AvalancheMQ::AMQP::Field){"s" => "båäö€", "i32" => 123, "u" => 0_u8}
    t = Time.epoch(Time.utc_now.epoch)
    props = AvalancheMQ::AMQP::Properties.new("application/json", "gzip", h, 1_u8, 9_u8, "correlation_id", "reply_to", "1000", "message_id", t, "type", "user_id", "app_id", "reserved1")
    io.write_bytes props, IO::ByteFormat::NetworkEndian
    io.pos.should eq props.bytesize
    io.pos = 0
    props2 = AvalancheMQ::AMQP::Properties.decode(io)
    props2.should eq props
  end
end
