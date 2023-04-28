require "log"

struct Log::Entry
  getter fiber : String? = Fiber.current.name
end
