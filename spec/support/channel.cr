#
# Add support to ::Channel for doing
#
# ```
# ch = Channel(String).new
# ch.should be_recieving "message"
# ```
#
# and
#
# ```
# ch = Channel(String).new
# ch.should be_sending "message"
# ```
#
module Spec::Expectations
  macro be_receiving(value, *, timeout = 5.seconds)
    ChannelReceiveExpectation.new({{value}}, {{timeout}})
  end

  macro be_sending(value, *, timeout = 5.seconds)
    ChannelSendExpectation.new({{value}}, {{timeout}})
  end
end

abstract class ChannelExpectation
  abstract def match(channel : ::Channel) : Bool
  abstract def failure_message : String
  abstract def negative_failure_message : String
end

class ChannelReceiveExpectation(T) < ChannelExpectation
  @wrong_value : T? = nil

  def initialize(@value : T?, @timeout : Time::Span = 5.seconds)
  end

  def match(channel : ::Channel) : Bool
    select
    when value = channel.receive
      if value == @value
        true
      else
        @wrong_value = value
        false
      end
    when timeout(@timeout)
      false
    end
  end

  def failure_message : String
    if wrong_value = @wrong_value
      "Expected channel to receive '#{@value}' but got '#{wrong_value}'"
    else
      "Timeout! Expected channel to receive '#{@value}' within #{@timeout}"
    end
  end

  def negative_failure_message : String
    "Expected channel not to receive '#{@value}' within #{@timeout}"
  end
end

class ChannelSendExpectation(T) < ChannelExpectation
  def initialize(@value : T, @timeout : Time::Span = 5.seconds)
  end

  def match(channel : ::Channel) : Bool
    select
    when channel.send(@value)
      true
    when timeout(@timeout)
      false
    end
  end

  def failure_message : String
    "Expected channel to accept send within #{@timeout}"
  end

  def negative_failure_message : String
    "Timeout! Expected channel not to accept send within #{@timeout}"
  end
end

class ::Channel
  def should(expectation : ChannelExpectation, *, file = __FILE__, line = __LINE__)
    unless expectation.match(self)
      fail(expectation.failure_message, file, line)
    end
  end

  def should_not(expectation : ChannelExpectation, *, file = __FILE__, line = __LINE__)
    if expectation.match(self)
      fail(expectation.negative_failure_message, file, line)
    end
  end
end
