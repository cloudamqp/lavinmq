require "./spec_helper"

describe LavinMQ::JournalLogFormat do
  it "writes the exception message and its cause's message" do
    ex = RuntimeError.new("outer failure", cause: IO::Error.new("inner failure"))
    entry = Log::Entry.new("spec", :error, "Read Loop error", Log::Metadata.empty, ex)
    io = IO::Memory.new
    LavinMQ::JournalLogFormat.format(entry, io)
    lines = io.to_s.lines.reject(&.includes?("  from "))
    lines.should eq [
      "<3> spec Read Loop error",
      "<3> outer failure (RuntimeError)",
      "<3> Caused by: inner failure (IO::Error)",
    ]
  end

  it "escapes line breaks in the exception message" do
    ex = RuntimeError.new("line one\nline two\r\nline three")
    entry = Log::Entry.new("spec", :error, "Read Loop error", Log::Metadata.empty, ex)
    io = IO::Memory.new
    LavinMQ::JournalLogFormat.format(entry, io)
    io.to_s.lines.should eq [
      "<3> spec Read Loop error",
      "<3> line one\\nline two\\r\\nline three (RuntimeError)",
    ]
  end
end
