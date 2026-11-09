require "spec"
require "../src/stdlib/gc"

{% if flag?(:patched_libgc) %}
  lib LibGC
    fun set_log_fd = GC_set_log_fd(fd : LibC::Int)
    $print_stats = GC_print_stats : LibC::Int
  end
{% end %}

describe "libgc" do
  it "is stock unless switched on" do
    GC.grow_mark_stack?.should be_false
    {% if flag?(:patched_libgc) %}
      LibGC.get_grow_mark_stack.should eq 0
    {% end %}
  end

  it "pushes parked fiber stacks lazily when switched on and marking in parallel" do
    {% if flag?(:patched_libgc) %}
      with_refs_on_parked_stack do
        mark_stack_overflows { GC.collect }.should be > 0
        with_grow_mark_stack do
          LibGC.get_grow_mark_stack.should eq 1
          overflows = mark_stack_overflows { GC.collect }
          if LibGC.get_parallel == 0
            overflows.should be > 0
          else
            overflows.should eq 0
          end
        end
      end
    {% else %}
      pending!("stock libgc, build with make to link the patched one") unless ENV["CI"]?
      fail "built without -Dpatched_libgc"
    {% end %}
  end

  [false, true].each do |grow|
    it "keeps objects referenced only from parked fiber stacks (grow_mark_stack=#{grow})" do
      pending!("stock libgc") if grow && !GC::PATCHED
      fibers = 20_000
      done = Channel(Nil).new
      results = Channel(Bool).new
      fibers.times do |i|
        spawn do
          obj = Array.new(4, i)
          done.receive
          results.send obj.all?(i)
        end
      end
      Fiber.yield
      garbage = 0
      begin
        GC.grow_mark_stack = grow
        4.times do
          GC.collect
          garbage += Array.new(fibers) { Array.new(4, -1) }.size
        end
      ensure
        GC.grow_mark_stack = false
      end
      fibers.times { done.send nil }
      intact = Array.new(fibers) { results.receive }
      intact.count(false).should eq 0
      garbage.should eq fibers * 4
    end
  end
end

REFS = 400_000

def with_refs_on_parked_stack(&)
  ready = Channel(Nil).new
  done = Channel(Nil).new
  intact = Channel(Bool).new
  spawn do
    refs = uninitialized StaticArray(Int64*, REFS)
    ptr = refs.to_unsafe
    REFS.times { |i| (ptr[i] = GC.malloc(16).as(Int64*)).value = i.to_i64 }
    ready.send nil
    done.receive
    intact.send (0...REFS).all? { |i| ptr[i].value == i }
  end
  ready.receive
  begin
    yield
  ensure
    done.send nil
  end
  Array.new(REFS) { GC.malloc(16).as(Int64*).value = -1_i64 }
  intact.receive.should be_true
end

def with_grow_mark_stack(&)
  GC.grow_mark_stack = true
  yield
ensure
  GC.grow_mark_stack = false
end

{% if flag?(:patched_libgc) %}
  def mark_stack_overflows(&) : Int32
    log = File.tempfile("gc")
    LibGC.set_log_fd(log.fd)
    LibGC.print_stats = 1
    begin
      yield
    ensure
      LibGC.print_stats = 0
      LibGC.set_log_fd(2)
    end
    File.read(log.path).scan("Mark stack overflow").size
  ensure
    log.try &.close
    log.try &.delete
  end
{% end %}
