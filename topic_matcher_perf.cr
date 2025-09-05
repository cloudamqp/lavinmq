tests = [
  ["*.test",
   "rk2.test"],

  ["rk1",
   "rk1"],

  ["*",
   "rk2"],

  ["*",
   "rk2.a"],

  ["a.*",
   "b.c"],

  ["c.*.d",
   "c.a.d"],

  ["d.#",
   "d.a.d"],

  ["rk63.rk63",
   "rk63"],

  ["#.a",
   "a.a.a"],

  ["#.*.d",
   "a.a.d",
  ],

  ["#.a.#",
   "b.b.a.b.b"],

  ["c.*.*",
   "c.a.d"],

  ["c.*",
   "c.a.d"],

  ["c.*.*.*",
   "c.a.d.e"],
]

require "benchmark"

class Rk
  abstract struct Segment
    abstract def match?(v) : Bool
  end

  struct HashSegment < Segment
    def match?(v) : Bool
      true
    end
  end

  struct StarSegment < Segment
    def match?(v) : Bool
      true
    end
  end

  struct StringSegment < Segment
    def initialize(@s : String)
    end

    def match?(v) : Bool
      v == @s
    end
  end

  @parts : Array(Segment)

  def initialize(bk : Array(String))
    # puts "bk=#{rk}"
    @parts = bk.map do |v|
      case v
      when "#" then HashSegment.new
      when "*" then StarSegment.new
      else          StringSegment.new(v)
      end
    end
  end

  def matches?(rk : String)
    i = 0
    # puts "rk=#{rk}"
    prev_checker = nil
    return true
    parts = rk.split(".")
    while i < parts.size
      seg = parts[i] # .each do |seg|
      if checker = @parts[i]?
        # puts "seg=\"#{seg}\" checker=#{checker}"
        prev_checker = checker
        return false unless checker.match?(seg)
      else
        if prev_checker.try(&.is_a?(HashSegment))
          # puts "seg=\"#{seg}\" ok becahse Hash before"
        else
          return false
        end
      end
      return false
      i += 1
    end
    return false if @parts[i]?
    true
  end
end

def old_topic_match(bks : Array(String), routing_key : String)
  rk_parts = routing_key.split(".")
  ok = false
  prev_hash = false
  size = bks.size # binding keys can max be 256 chars long anyway
  j = 0
  i = 0
  bks.each do |part|
    if rk_parts.size <= j
      ok = false
      break
    end
    case part
    when "#"
      j += 1
      prev_hash = true
      ok = true
    when "*"
      prev_hash = false
      # Is this the last bk and the last rk?
      if size == i + 1 && rk_parts.size == j + 1
        ok = true
        break
        # More than 1 rk left ok move on
      elsif rk_parts.size > j + 1
        j += 1
        i += 1
        next
      else
        ok = false
        j += 1
      end
    else
      if prev_hash
        if size == (i + 1)
          ok = rk_parts.last == part
          j += 1
        else
          ok = false
          rk_parts[j..-1].each do |rk_part|
            j += 1
            ok = part == rk_part
            break if ok
          end
        end
      else
        # Is this the last bk but not the last rk?
        if size == i + 1 && rk_parts.size > j + 1
          ok = false
        else
          ok = rk_parts[j] == part
        end
        j += 1
      end
    end
    break unless ok
    i += 1
  end
end

Benchmark.ips do |x|
  x.report("old") do
    tests.each do |v|
      parts = v[0].split(".")
      old_topic_match(parts, v[1])
    end
  end

  x.report("new") do
    tests.each do |v|
      parts = v[0].split(".")
      rk = Rk.new(parts)
      # puts "new matches=#{rk.matches?(routing_key)}"
      rk.matches? v[1]
    end
  end
end
