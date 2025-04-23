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
  rescue Channel::ClosedError
    Log.debug { "BoolChannel closed, could not set value" }
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
      when channel.send nil
      when value = @value.receive
      end
    end
  rescue Channel::ClosedError
  end
end

class StateChannel(T)
  getter value : T
  getter change = Channel(T).new
  @new_value = Channel(T).new

  def initialize(@value : T)
    spawn(name: "StateChannel#send_loop") do
      send_loop(@value)
    end
  end

  def set(value : Bool)
    @value = value
    @new_value.send value
  end

  def close
    @change.close
    @new_value.close
  end

  private def send_loop(value)
    loop do
      select
      when @change.send value
      when value = @new_value.receive
      end
    end
  rescue Channel::ClosedError
  end
end
