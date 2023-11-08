abstract class Crystal::EventLoop
  def stop
  end
end

{% if flag?(:unix) %}
  class Crystal::LibEvent::EventLoop
    def stop
      if event_base = @event_base
        # dunno if break is needed?
        event_base.loop_break
        event_base.free
      end
    end
  end
{% end %}
