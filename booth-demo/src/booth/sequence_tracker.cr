module Booth
  # Checks a stream of sequence numbers for gaps and duplicates. A gap may
  # fill in later (a republish after a failover), so a missing number only
  # counts as lost once it's been missing for a grace period and the
  # publisher has it confirmed.
  class SequenceTracker
    # Bigger jumps than this are treated as a restart, not a gap
    MAX_GAP = 1_000_000

    getter received = 0i64
    getter duplicates = 0i64
    getter lost = 0i64
    getter highest = 0i64
    @missing = Hash(Int64, Time::Instant).new

    def receive(seq : Int64, now = Time.instant) : Nil
      @received += 1
      if seq > @highest
        if seq - @highest <= MAX_GAP
          (@highest + 1).upto(seq - 1) { |s| @missing[s] = now }
        end
        @highest = seq
      elsif @missing.delete(seq).nil?
        @duplicates += 1
      end
    end

    def missing : Int32
      @missing.size
    end

    # Moves numbers missing for longer than *grace*, that *confirmed* says
    # the broker confirmed, to lost
    def check(grace : Time::Span, now = Time.instant, & : Int64 -> Bool) : Nil
      @missing.reject! do |seq, since|
        if now - since > grace && yield(seq)
          @lost += 1
          true
        else
          false
        end
      end
    end
  end
end
