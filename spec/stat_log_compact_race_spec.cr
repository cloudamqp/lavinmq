require "./spec_helper"

# Memory-safety stress for the compact path: a writer repeatedly materializes
# then compacts an object's stats buffer (compact nulls the buffer; GC.collect +
# GC_UNMAP_THRESHOLD=1 can unmap it) while reader fibers iterate StatLogViews to
# JSON (the HTTP read path). If a view did not pin the buffer via its BASE
# pointer, a compacted+unmapped buffer would be dereferenced -> SIGSEGV.
#
# Uses 3 rate columns so columns 1 and 2 sit at INTERIOR offsets (base + i*cap),
# the exact case the base-pointer storage protects. Run the adversarial control
# to prove no dependence on interior-pointer recognition:
#   GC_UNMAP_THRESHOLD=1 GC_FREE_SPACE_DIVISOR=1 crystal spec ... (must pass)
#   GC_UNMAP_THRESHOLD=1 GC_ALL_INTERIOR_POINTERS=0 crystal spec ... (must also pass)
module LavinMQ
  class RaceProbe
    include Stats
    rate_stats({"a", "b", "c"}, {"d"})

    @d = 0_u32

    def d
      @d
    end

    def bump(n : UInt64)
      @a_count.add(n)
      @b_count.add(n)
      @c_count.add(n)
      @d &+= 1
    end

    def constant?
      @_stats_rate_buffer.null?
    end
  end
end

describe "StatLogView compact/read race" do
  it "never frees a buffer out from under a concurrent reader" do
    cap = LavinMQ::Config.instance.stats_log_size
    probe = LavinMQ::RaceProbe.new
    stop = Atomic(Int32).new(0)
    errors = Atomic(Int32).new(0)
    done = Channel(Nil).new
    readers = 8

    readers.times do
      spawn do
        until stop.get(:relaxed) == 1
          begin
            io = IO::Memory.new
            jb = JSON::Builder.new(io)
            jb.document do
              jb.array do
                probe.a_log.to_json(jb) # column 0 (base)
                probe.b_log.to_json(jb) # column 1 (interior)
                probe.c_log.to_json(jb) # column 2 (interior)
                probe.d_log.to_json(jb) # count column
              end
            end
            probe.b_log.to_a
            probe.c_log.each { |v| v }
          rescue
            errors.add(1)
          end
          Fiber.yield
        end
        done.send(nil)
      end
    end

    materializes = 0
    compacts = 0
    300.times do |c|
      probe.bump(c.odd? ? 7_u64 : 131_u64) # changing delta -> rate diverges -> materialize
      probe.update_rates
      materializes += 1 unless probe.constant?
      (cap + 1).times { probe.update_rates } # constant (no bump) -> rate holds -> compact
      compacts += 1 if probe.constant?
      GC.collect # with GC_UNMAP_THRESHOLD=1 may unmap a just-freed buffer
      Fiber.yield
    end

    stop.set(1, :relaxed)
    readers.times { done.receive }

    errors.get(:relaxed).should eq 0
    (materializes > 100).should be_true # really exercised buffered logs
    (compacts > 100).should be_true     # ...and really compacted them
  end
end
