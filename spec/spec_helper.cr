require "spec"
require "file_utils"
require "../src/avalanchemq/server"
module TestHelpers
  def wait_for
    timeout = Time.now + 1.seconds
    until yield
      Fiber.yield
      if Time.now > timeout
        puts "Execuction expired"
        return
      end
    end
  end
end
extend TestHelpers
