require "../../src/lavinmq/bool_channel"

# Add assertions to bool channel to make it testable in a nice way
class BoolChannel
  private def _spec_value
    select
    when when_true.receive
      true
    when when_false.receive
      false
    end
  end

  def should(expectation : Spec::EqualExpectation, failure_message : String? = nil, *, file = __FILE__, line = __LINE__)
    value = _spec_value
    unless expectation.match value
      failure_message ||= expectation.failure_message(value)
      fail(failure_message, file, line)
    end
  end

  def should_not(expectation, failure_message : String? = nil, *, file = __FILE__, line = __LINE__)
    value = _spec_value
    if expectation.match value
      failure_message ||= expectation.negative_failure_message(value)
      fail(failure_message, file, line)
    end
  end
end
