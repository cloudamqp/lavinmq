require "log"

struct Log::Entry
  {% if flag?(:preview_mt) %}
    THREAD_NUMBERS = Hash(UInt64, Int32).new { |h, k| h[k] = h.size }
    getter thread_id : Int32 = THREAD_NUMBERS[Thread.current.object_id]
  {% end %}

  getter fiber : String? = Fiber.current.name
end
