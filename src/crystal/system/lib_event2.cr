{% if flag?(:unix) %}
  lib LibEvent2
    fun event_base_free(event : EventBase) : Nil
  end
{% end %}
