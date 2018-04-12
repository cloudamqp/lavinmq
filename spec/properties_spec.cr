require "./spec_helper"

describe AvalancheMQ::AMQP::Table do
  it "can be encoded and decoded" do
    t = Time.epoch_ms(Time.utc_now.epoch_ms)
    data = Hash(String, AvalancheMQ::AMQP::Field){
      "s" => "åäö😀",
      "a" => Array(AvalancheMQ::AMQP::Field){ "€", 1_u16, 23, t, true, 1.2 },
      "h" => Hash(String, AvalancheMQ::AMQP::Field){ "i" => "y" },
      "u" => 0_u8
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
    h = Hash(String, AvalancheMQ::AMQP::Field){ "s" => "båäö€", "i32" => 123, "u" => 0_u8 }
    t = Time.epoch_ms(Time.utc_now.epoch_ms)
    props = AvalancheMQ::AMQP::Properties.new("application/json", "gzip", h, 1_u8, 9_u8, "correlation_id", "reply_to", "1000", "message_id", t, "type", "user_id", "app_id", "reserved1")
    io.write_bytes props, IO::ByteFormat::NetworkEndian
    io.pos.should eq props.bytesize
    io.pos = 0
    props2 = AvalancheMQ::AMQP::Properties.decode(io)
    props2.should eq props
  end
end
