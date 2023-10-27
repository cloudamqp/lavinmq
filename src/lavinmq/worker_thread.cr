{% raise "Can't use WorkerThread without -Dpreview_mt" unless flag?(:preview_mt) %}
require "crystal/system/thread"

# Extend FiberChannel to enable us to stop scheduler
struct Crystal::FiberChannel
  class IOError < IO::Error; end

  def send(fiber : Fiber)
    previous_def
  rescue e : IO::Error
    raise IOError.new(cause: e)
  end

  def receive
    previous_def
  rescue e : IO::Error
    raise IOError.new(cause: e)
  end

  def close
    @worker_in.close unless @worker_in.closed?
  end
end

module LavinMQ
  class WorkerScheduler < Crystal::Scheduler
    @stopped : Channel(Nil)? = nil

    def run_loop
      super
    rescue e : Crystal::FiberChannel::IOError
      @stopped.try &.send(nil)
    end

    def stop(@stopped : Channel(Nil)? = nil)
      self.fiber_channel.close
    end
  end

  class WorkerThread < Thread
    Log = ::Log.for("lavinmq.worker_thread")

    def initialize(&func : ->)
      Log.info { "Started #{object_id}" }
      super(&func)
    end

    def spawn(name : String, &block)
      Fiber.new(name: name, &block).tap do |f|
        f.@current_thread.set(self)
        f.enqueue
      end
    end

    def scheduler : Crystal::Scheduler
      @scheduler ||= WorkerScheduler.new(main_fiber)
    end

    def stop
      Log.info { "Stopping #{object_id}" }
      if s = scheduler.as?(WorkerScheduler)
        s_stopped = Channel(Nil).new
        s.stop(s_stopped)
        s_stopped.receive
      end
      Log.info { "Stopped #{object_id}" }
      self.join
    end
  end
end
