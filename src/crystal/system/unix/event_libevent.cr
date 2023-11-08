{% if flag?(:unix) %}
  struct Crystal::LibEvent::Event::Base
    @freed = false

    def free : Nil
      return if @freed
      @freed = true
      LibEvent2.event_base_free(@base)
    end
  end
{% end %}
