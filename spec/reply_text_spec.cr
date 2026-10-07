require "./spec_helper"

describe LavinMQ::AMQP::ReplyText do
  # A reply_text is a short string, so it cannot exceed 255 bytes. The text is
  # composed from a reply code and a message that can include client-supplied
  # names, so it has to be capped where it is built, not at the call sites.
  it "keeps the connection when a channel error reply_text would exceed 255 bytes" do
    with_amqp_server do |s|
      with_raw_amqp_connection(s) do |io, stream|
        io.write_bytes AMQ::Protocol::Frame::Channel::Open.new(1_u16), IO::ByteFormat::NetworkEndian
        io.flush
        stream.next_frame.as(AMQ::Protocol::Frame::Channel::OpenOk)

        # Queue names are legal up to 255 bytes, and the server answers a
        # passive declare of a missing queue with a text that embeds the name.
        long_name = "q" * 240
        io.write_bytes AMQ::Protocol::Frame::Queue::Declare.new(1_u16, 0_u16, long_name,
          true, false, false, false, false, AMQ::Protocol::Table.new), IO::ByteFormat::NetworkEndian
        io.flush

        close = stream.next_frame.as(AMQ::Protocol::Frame::Channel::Close)
        close.reply_code.should eq 404
        close.reply_text.bytesize.should be <= 255
        close.reply_text.should start_with "NOT_FOUND - "
        close.reply_text.should contain "qqq"

        # The frame was well formed, so the connection is still usable.
        io.write_bytes AMQ::Protocol::Frame::Channel::CloseOk.new(1_u16), IO::ByteFormat::NetworkEndian
        io.write_bytes AMQ::Protocol::Frame::Channel::Open.new(2_u16), IO::ByteFormat::NetworkEndian
        io.flush
        stream.next_frame.should be_a(AMQ::Protocol::Frame::Channel::OpenOk)
      end
    end
  end

  it "caps the reply_text when closing a channel with a long X-Reason" do
    with_http_server do |http, s|
      with_raw_amqp_connection(s) do |io, stream|
        io.write_bytes AMQ::Protocol::Frame::Channel::Open.new(1_u16), IO::ByteFormat::NetworkEndian
        io.flush
        stream.next_frame.as(AMQ::Protocol::Frame::Channel::OpenOk)

        name = URI.encode_path(JSON.parse(http.get("/api/channels").body)[0]["name"].as_s)
        hdrs = ::HTTP::Headers{"X-Reason" => "R" * 300}
        http.delete("/api/channels/#{name}", headers: hdrs).status_code.should eq 204

        close = stream.next_frame.as(AMQ::Protocol::Frame::Channel::Close)
        close.reply_text.bytesize.should be <= 255
        close.reply_text.should start_with "PRECONDITION_FAILED - "
        close.reply_text.should contain "RRR"
      end
    end
  end

  it "caps the reply_text when closing a connection with a long X-Reason" do
    with_http_server do |http, s|
      with_raw_amqp_connection(s) do |io, stream|
        io.write_bytes AMQ::Protocol::Frame::Channel::Open.new(1_u16), IO::ByteFormat::NetworkEndian
        io.flush
        stream.next_frame.as(AMQ::Protocol::Frame::Channel::OpenOk)

        name = URI.encode_path(JSON.parse(http.get("/api/connections").body)[0]["name"].as_s)
        hdrs = ::HTTP::Headers{"X-Reason" => "R" * 300}
        http.delete("/api/connections/#{name}", headers: hdrs).status_code.should eq 204

        # The connection is meant to close here. What matters is that the
        # client can still read the frame and learn why.
        close = stream.next_frame.as(AMQ::Protocol::Frame::Connection::Close)
        close.reply_text.bytesize.should be <= 255
        close.reply_text.should start_with "CONNECTION_FORCED - "
        close.reply_text.should contain "RRR"
      end
    end
  end

  it "caps the reply_text when the vhost is refused during connection open" do
    with_amqp_server do |s|
      # The refusal embeds the vhost name, which is a short string itself, so
      # the reply_text overflows before it reaches the socket.
      vhost = "v" * 240
      s.vhosts.create(vhost)
      s.users.rm_permission("guest", vhost)

      io = TCPSocket.new("localhost", amqp_port(s))
      io.read_timeout = 5.seconds
      begin
        io.write AMQ::Protocol::PROTOCOL_START_0_9_1.to_slice
        io.flush
        stream = AMQ::Protocol::Stream.new(io)
        stream.next_frame.as(AMQ::Protocol::Frame::Connection::Start)
        io.write_bytes AMQ::Protocol::Frame::Connection::StartOk.new(
          AMQ::Protocol::Table.new, "PLAIN", "\u0000guest\u0000guest", ""),
          IO::ByteFormat::NetworkEndian
        io.flush
        tune = stream.next_frame.as(AMQ::Protocol::Frame::Connection::Tune)
        io.write_bytes AMQ::Protocol::Frame::Connection::TuneOk.new(
          channel_max: tune.channel_max, frame_max: tune.frame_max, heartbeat: 0_u16),
          IO::ByteFormat::NetworkEndian
        io.write_bytes AMQ::Protocol::Frame::Connection::Open.new(vhost), IO::ByteFormat::NetworkEndian
        io.flush

        close = stream.next_frame.as(AMQ::Protocol::Frame::Connection::Close)
        close.reply_text.bytesize.should be <= 255
        close.reply_text.should start_with "NOT_ALLOWED - "
        close.reply_text.should contain "vvv"
      ensure
        io.close
      end
    end
  end

  describe ".build" do
    it "drops a multi-byte character rather than splitting it" do
      # "X - " plus 250 bytes puts the 2-byte character across the 255 limit.
      built = LavinMQ::AMQP::ReplyText.build("X", ("a" * 250) + "ä")
      built.bytesize.should be <= 255
      built.valid_encoding?.should be_true
      built.should eq "X - " + ("a" * 250)
    end

    it "leaves a text that already fits" do
      LavinMQ::AMQP::ReplyText.build("X", "short").should eq "X - short"
    end
  end
end
