require "log"
require "../stdlib/channel"

# A `Log::Backend` for holding last x nr of messages in memory.
class Log::InMemoryBackend < Log::Backend
  @entries = Deque(Log::Entry).new
  @lock = Mutex.new
  getter channels = Array(Channel(Log::Entry)).new
  property size = 1024

  @@instance : self = self.new

  def entries
    @lock.synchronize do
      @entries.dup
    end
  end

  def self.instance
    @@instance
  end

  private def initialize
    super(:direct)
  end

  def write(entry : Log::Entry) : Nil
    @lock.synchronize do
      if @entries.size >= @size
        @entries.shift(@entries.size - @size + 1)
      end
      @entries << entry

      @channels.each do |ch|
        ch.try_send(entry)
      end
    end
  end

  def add_channel
    ch = Channel(Log::Entry).new(128)
    @lock.synchronize do
      @channels << ch
    end
    ch
  end

  def remove_channel(channel)
    @lock.synchronize do
      @channels.delete(channel)
    end
  end
end
