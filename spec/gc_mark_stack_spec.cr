{% skip_file if flag?(:darwin) %} # Homebrew's libgc is shared and unpatched
require "spec"

lib LibGC
  fun set_log_fd = GC_set_log_fd(fd : LibC::Int)
end

# The packages link a patched libgc (packaging/libgc/). Stock bdwgc 8.2.12 with
# parallel marking never grows the global mark stack when the roots pushed for
# thousands of parked fibers overflow it, so every collection rescans the heap.
describe "libgc" do
  it "grows the mark stack when parked fibers overflow it" do
    # GC_print_stats is a hidden symbol in a shared libgc, so look it up at runtime
    print_stats = LibC.dlsym(LibC::RTLD_DEFAULT, "GC_print_stats").as(LibC::Int*)
    pending!("libgc without GC_print_stats") if print_stats.null?
    done = Channel(Nil).new
    fibers = 20_000
    fibers.times { spawn { done.receive } }
    Fiber.yield
    log = File.tempfile("gc")
    overflows = Array.new(4) do
      log.truncate
      log.rewind
      LibGC.set_log_fd(log.fd)
      print_stats.value = 1
      begin
        GC.collect
      ensure
        print_stats.value = 0
        LibGC.set_log_fd(2)
      end
      File.read(log.path).scan("Mark stack overflow").size
    end
    log.close
    log.delete
    fibers.times { done.send nil }
    # The stack never shrinks, so only the last collection says whether it grew
    if overflows.last > 0 && !ENV["CI"]?
      pending!("stock libgc, see packaging/libgc/build.sh")
    end
    overflows.last.should eq 0
  end
end
