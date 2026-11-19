require "./spec_helper"

describe LavinMQ::JournalLogFormat do
  it "writes the exception message, its backtrace and its cause's message, each line with a priority" do
    ex = begin
      raise RuntimeError.new("outer failure", cause: IO::Error.new("inner failure"))
    rescue e
      e
    end
    entry = Log::Entry.new("spec", :error, "Read Loop error", Log::Metadata.empty, ex)
    io = IO::Memory.new
    LavinMQ::JournalLogFormat.format(entry, io)
    lines = io.to_s.lines
    lines.first(2).should eq ["<3> spec Read Loop error", "<3> outer failure (RuntimeError)"]
    lines.last.should eq "<3> Caused by: inner failure (IO::Error)"
    frames = lines[2...-1]
    frames.should_not be_empty
    frames.each(&.should(start_with("<3>   from ")))
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
