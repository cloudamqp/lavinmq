module AMQPServer
  class Backend
    def initialize
      @connection_commands = Channel(Nil).new
      @frame_channel = Channel(AMQP::Frame?).new(1)
      @open_channels = Set(UInt16).new
    end

    def process_frame(frame : AMQP::Frame)
      case frame
      when AMQP::Channel::Open
        @open_channels.add frame.channel
        @frame_channel.send AMQP::Channel::OpenOk.new(frame.channel)
      when AMQP::Channel::Close
        @open_channels.delete frame.channel
        @frame_channel.send AMQP::Channel::CloseOk.new(frame.channel)
      when AMQP::Exchange::Declare
        @open_channels.delete frame.channel
        @frame_channel.send AMQP::Exchange::DeclareOk.new(frame.channel)
      when AMQP::Connection::Close
        @frame_channel.send AMQP::Connection::CloseOk.new
      end
    end

    def next_frame
      @frame_channel.receive_select_action
    end
  end
end
