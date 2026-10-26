require "spec"
require "http/server"
require "../../src/stdlib/http_server"

private class SpyOutput < IO
  getter byte_writes = 0
  getter slice_writes = 0

  def read(slice : Bytes) : NoReturn
    raise "Can't read"
  end

  def write(slice : Bytes) : Nil
    @slice_writes += 1
  end

  def write_byte(byte : UInt8) : Nil
    @byte_writes += 1
  end
end

describe HTTP::Server::Response do
  it "forwards write_byte to its output" do
    response = HTTP::Server::Response.new(IO::Memory.new)
    output = SpyOutput.new
    response.output = output
    response.write_byte 'a'.ord.to_u8
    output.byte_writes.should eq 1
    output.slice_writes.should eq 0
  end
end
