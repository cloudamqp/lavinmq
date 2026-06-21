require "spec"
require "../../src/lavinmq/amqp10/codec"

include LavinMQ::AMQP10

# `Symbol` at the top level resolves to Crystal's ::Symbol; alias the AMQP one.
alias Sym = LavinMQ::AMQP10::Symbol

private def roundtrip(v : Codec::AnyValue) : Codec::AnyValue
  io = IO::Memory.new
  Codec.write(io, v)
  io.rewind
  Codec.read(io)
end

describe LavinMQ::AMQP10::Codec do
  it "encodes and decodes null" do
    roundtrip(nil).should be_nil
  end

  it "encodes and decodes booleans" do
    roundtrip(true).should eq true
    roundtrip(false).should eq false
  end

  it "encodes and decodes unsigned integers (normalised to UInt64)" do
    roundtrip(0_u64).should eq 0_u64
    roundtrip(200_u64).should eq 200_u64
    roundtrip(70_000_u64).should eq 70_000_u64
    roundtrip(UInt64::MAX).should eq UInt64::MAX
  end

  it "encodes and decodes signed integers (normalised to Int64)" do
    roundtrip(0_i64).should eq 0_i64
    roundtrip(-1_i64).should eq -1_i64
    roundtrip(-100_i64).should eq -100_i64
    roundtrip(Int64::MIN).should eq Int64::MIN
    roundtrip(Int64::MAX).should eq Int64::MAX
  end

  it "encodes and decodes floats" do
    roundtrip(1.5_f32).should eq 1.5_f32
    roundtrip(3.14159_f64).should eq 3.14159_f64
  end

  it "encodes and decodes strings" do
    roundtrip("").should eq ""
    roundtrip("hello").should eq "hello"
    long = "x" * 1000
    roundtrip(long).should eq long
  end

  it "encodes and decodes symbols" do
    roundtrip(Sym.new("amqp:accepted:list")).should eq Sym.new("amqp:accepted:list")
  end

  it "encodes and decodes binary" do
    bytes = Bytes[1, 2, 3, 255, 0]
    roundtrip(bytes).should eq bytes
  end

  it "encodes and decodes UUIDs" do
    u = UUID.new("123e4567-e89b-12d3-a456-426614174000")
    roundtrip(u).should eq u
  end

  it "encodes and decodes timestamps at millisecond precision" do
    t = Time.unix_ms(1_700_000_000_123)
    roundtrip(t).should eq t
  end

  it "encodes and decodes empty and non-empty lists" do
    roundtrip([] of Codec::AnyValue).should eq([] of Codec::AnyValue)
    list = [1_u64, "two", nil, true] of Codec::AnyValue
    roundtrip(list).should eq list
  end

  it "encodes and decodes maps" do
    map = Hash(Codec::AnyValue, Codec::AnyValue){
      Sym.new("key") => "value",
      "n"            => 42_u64,
    }
    roundtrip(map).should eq map
  end

  it "encodes and decodes described types" do
    d = Described.new(0x24_u64, [] of Codec::AnyValue)
    rt = roundtrip(d).as(Described)
    rt.descriptor.should eq 0x24_u64
    rt.value.should eq([] of Codec::AnyValue)
  end

  it "decodes a large (32-bit) list" do
    list = Array(Codec::AnyValue).new(300, &.to_u64)
    roundtrip(list).should eq list
  end
end
