require "./spec_helper"

describe AvalancheMQ::AMQP::Table do
  it "can be encoded and decoded" do
    io = IO::Memory.new
    data = Hash(String, AvalancheMQ::AMQP::Field){ "h" => "åäö😀", "a" => Array(AvalancheMQ::AMQP::Field){ "€", 1_u16, 23 }}
    tbl = AvalancheMQ::AMQP::Table.new(data)
    io.write_bytes tbl, IO::ByteFormat::NetworkEndian
    io.pos.should eq(tbl.bytesize + 4)
    io.pos = 0
    data2 = AvalancheMQ::AMQP::Table.from_io(io, IO::ByteFormat::NetworkEndian)
    data.should eq data2
  end
end

describe AvalancheMQ::AMQP::Properties do
  it "can be encoded and decoded" do
    io = IO::Memory.new
    h = Hash(String, AvalancheMQ::AMQP::Field){ "h" => "båäö€", "b" => 123, "a" => 0_u8 }
    t = Time.epoch_ms(Time.utc_now.epoch_ms)
    props = AvalancheMQ::AMQP::Properties.new("application/json", "gzip", h, 1_u8, 9_u8, "correlation_id", "reply_to", "1000", "message_id", t, "type", "user_id", "app_id", "reserved1")
    io.write_bytes props, IO::ByteFormat::NetworkEndian
    io.pos.should eq props.bytesize
    io.pos = 0
    props2 = AvalancheMQ::AMQP::Properties.from_io(io, IO::ByteFormat::NetworkEndian)
    props2.should eq props
  end
end
