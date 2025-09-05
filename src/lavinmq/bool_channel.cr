# A class which has two channels, one for which a state is true
# and the other for when the state if false
class BoolChannel
  getter when_true = Channel(Nil).new
  getter when_false = Channel(Nil).new
  @value = Channel(Bool).new

  def initialize(value : Bool)
    spawn(name: "BoolChannel#send_loop") do
      send_loop(value)
    end
  end

  def set(value : Bool)
    @value.send value
  rescue ex : Channel::ClosedError
    Log.debug(exception: ex) { "BoolChannel closed, could not set value" }
  end

  def close
    @when_true.close
    @when_false.close
    @value.close
  end

  private def send_loop(value)
    loop do
      channel = value ? @when_true : @when_false
      select
      when value = @value.receive
      when channel.send nil
        Fiber.yield # Improves fairness by allowing the receiving fiber to run instead of notifying all waiting fibers
      end
    end
  rescue Channel::ClosedError
  end
end
