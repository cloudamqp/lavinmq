module MQTT
  module Protocol
    abstract struct Packet
      def to_slice
        io = ::IO::Memory.new
        self.to_io(IO.new(io))
        io.rewind
        io.to_slice
      end
    end
  end
end
