{% raise "Can't use WorkerThread without -Dpreview_mt" unless flag?(:preview_mt) %}
require "crystal/system/thread"
require "../crystal/fiber_channel.cr"
require "../crystal/system/lib_event2.cr"
require "../crystal/system/unix/event_libevent.cr"
require "../crystal/system/unix/event_loop_libevent.cr"

# Extend FiberChannel to enable us to stop scheduler
class Crystal::Scheduler
  @stopped : Channel(Nil)? = nil

  def run_loop
    fiber_channel = self.fiber_channel
    loop do
      @lock.lock
      if runnable = @runnables.shift?
        @runnables << Fiber.current
        @lock.unlock
        runnable.resume
      else
        @sleeping = true
        @lock.unlock
        unless fiber = fiber_channel.receive
          @stopped.try &.send(nil)
          return
        end
        @lock.lock
        @sleeping = false
        @runnables << Fiber.current
        @lock.unlock
        fiber.resume
      end
    end
  ensure
    @event_loop.stop
  end

  def stop(@stopped : Channel(Nil)? = nil)
    self.fiber_channel.close
  end
end

module LavinMQ
  class WorkerThread < Thread
    Log = ::Log.for("lavinmq.worker_thread")

    def initialize(&func : ->)
      Log.info { "Started #{object_id}" }
      super(&func)
    end

    def spawn(name : String, &block)
      f = Fiber.new(name: name, &block).tap do |f|
        f.@current_thread.set(self)
        f.enqueue
      end
    end

    def stop
      Log.info { "Stopping #{object_id}" }
      s_stopped = Channel(Nil).new
      scheduler.stop(s_stopped)
      s_stopped.receive
      Log.info { "Stopped #{object_id}" }
      self.join

      # Fiber.unsafe_each { |f| Log.info { "Fiber #{f.name} #{f.object_id.to_s(16)}" } }
      # Thread.unsafe_each { |t| Log.info { "Thread #{t.object_id.to_s(16)}" } }
    end
  end
end
