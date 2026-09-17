require "./spec_helper"

private def attr(json : String, name = "a")
  LavinMQ::SQS::MessageAttribute.parse(name, JSON.parse(json), "MessageAttributes")
end

describe LavinMQ::SQS::MessageAttribute do
  it "parses string, number and binary attributes" do
    s = attr(%({"DataType": "String", "StringValue": "hello"}))
    s.string_value.should eq "hello"
    s.binary?.should be_false
    n = attr(%({"DataType": "Number", "StringValue": "42"}))
    n.data_type.should eq "Number"
    b = attr(%({"DataType": "Binary", "BinaryValue": "#{Base64.strict_encode("\x00\x01\xff")}"}))
    b.binary?.should be_true
    b.binary_value.should eq Bytes[0, 1, 255]
    c = attr(%({"DataType": "String.custom-type", "StringValue": "x"}))
    c.data_type.should eq "String.custom-type"
  end

  it "rejects invalid attributes" do
    expect_raises(LavinMQ::SQS::InvalidParameterValue) { attr(%({"StringValue": "x"})) }
    expect_raises(LavinMQ::SQS::InvalidParameterValue) { attr(%({"DataType": "Integer", "StringValue": "1"})) }
    expect_raises(LavinMQ::SQS::InvalidParameterValue) { attr(%({"DataType": "String"})) }
    expect_raises(LavinMQ::SQS::InvalidParameterValue) { attr(%({"DataType": "String", "StringValue": ""})) }
    expect_raises(LavinMQ::SQS::InvalidParameterValue) { attr(%({"DataType": "Binary", "StringValue": "x"})) }
    expect_raises(LavinMQ::SQS::InvalidParameterValue) { attr(%({"DataType": "Binary", "BinaryValue": "%%%"})) }
  end

  it "validates names" do
    v = ->(name : String) { LavinMQ::SQS::MessageAttribute.validate_name!(name, "MessageAttributes") }
    v.call("valid.name-1_x")
    expect_raises(LavinMQ::SQS::InvalidParameterValue) { v.call("AWS.reserved") }
    expect_raises(LavinMQ::SQS::InvalidParameterValue) { v.call("amazon.reserved") }
    expect_raises(LavinMQ::SQS::InvalidParameterValue) { v.call(".leading") }
    expect_raises(LavinMQ::SQS::InvalidParameterValue) { v.call("trailing.") }
    expect_raises(LavinMQ::SQS::InvalidParameterValue) { v.call("double..dot") }
    expect_raises(LavinMQ::SQS::InvalidParameterValue) { v.call("has space") }
    expect_raises(LavinMQ::SQS::InvalidParameterValue) { v.call("a" * 257) }
  end

  it "limits the number of attributes" do
    map = Hash(String, JSON::Any).new
    11.times { |i| map["a#{i}"] = JSON.parse(%({"DataType": "String", "StringValue": "v"})) }
    expect_raises(LavinMQ::SQS::InvalidParameterValue, /exceeds/) do
      LavinMQ::SQS::MessageAttribute.parse_all(map)
    end
  end

  it "round trips through an AMQP table sorted by name" do
    attrs = [
      attr(%({"DataType": "String", "StringValue": "zed"}), "z"),
      attr(%({"DataType": "Binary", "BinaryValue": "#{Base64.strict_encode("bin")}"}), "b"),
      attr(%({"DataType": "Number", "StringValue": "1.5"}), "n"),
    ]
    table = LavinMQ::SQS::MessageAttribute.to_table(attrs)
    back = LavinMQ::SQS::MessageAttribute.from_table(table)
    back.map(&.name).should eq ["b", "n", "z"]
    back[0].binary_value.should eq "bin".to_slice
    back[1].string_value.should eq "1.5"
    back[1].data_type.should eq "Number"
    back[2].string_value.should eq "zed"
  end

  it "matches MessageAttributeNames selectors" do
    sel = ->(name : String, selectors : Array(String)) { LavinMQ::SQS::MessageAttribute.selected?(name, selectors) }
    sel.call("foo", ["All"]).should be_true
    sel.call("foo", [".*"]).should be_true
    sel.call("foo.bar", ["foo.*"]).should be_true
    sel.call("foo", ["foo"]).should be_true
    sel.call("bar", ["foo"]).should be_false
    sel.call("foobar", ["foo.*"]).should be_true
    sel.call("bar", ["foo.*"]).should be_false
  end
end

describe LavinMQ::SQS::Checksums do
  it "computes the md5 of the body" do
    LavinMQ::SQS::Checksums.md5_hex("This is a test message").should eq "fafb00f5732ab283681e124bf8747ed1"
  end

  it "computes MD5OfMessageAttributes like SQS does" do
    # Vector from the moto SQS test suite (a Number attribute named "timestamp")
    attrs = [attr(%({"DataType": "Number", "StringValue": "1493147359900"}), "timestamp")]
    LavinMQ::SQS::Checksums.md5_attributes(attrs).should eq "235c5c510d26fb653d073faed50ae77c"
  end

  it "encodes binary attributes with transport type 2" do
    attrs = [attr(%({"DataType": "Binary", "BinaryValue": "#{Base64.strict_encode("hi")}"}), "bin")]
    # name(4+3) type(4+6) transport(1) value(4+2)
    expected = IO::Memory.new
    expected.write_bytes(3_u32, IO::ByteFormat::BigEndian); expected << "bin"
    expected.write_bytes(6_u32, IO::ByteFormat::BigEndian); expected << "Binary"
    expected.write_byte(2_u8)
    expected.write_bytes(2_u32, IO::ByteFormat::BigEndian); expected << "hi"
    LavinMQ::SQS::Checksums.md5_attributes(attrs).should eq Digest::MD5.hexdigest(expected.to_slice)
  end

  it "returns nil when there are no attributes" do
    LavinMQ::SQS::Checksums.md5_attributes([] of LavinMQ::SQS::MessageAttribute).should be_nil
  end
end
