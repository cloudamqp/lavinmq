struct Time::MonthSpan
  include Comparable(self)
  include Comparable(Time::Span)

  def <=>(other : self)
    value <=> other.value
  end

  def <=>(other : Time::Span)
    value * 28 * 24 * 3600 <=> other.to_i
  end
end

struct Time::Span
  include Comparable(Time::MonthSpan)

  def <=>(other : Time::MonthSpan)
    to_i <=> other.value * 28 * 24 * 3600
  end
end
