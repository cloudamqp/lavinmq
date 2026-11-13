require "spec"
require "../src/stdlib/gc"

{% if flag?(:patched_libgc) %}
  lib LibGC
    fun set_log_fd = GC_set_log_fd(fd : LibC::Int)
    fun set_grow_mark_stack = GC_set_grow_mark_stack(value : Int)
    fun get_parallel = GC_get_parallel : Int
    $print_stats = GC_print_stats : LibC::Int
  end
{% end %}

describe "libgc" do
  it "pushes parked fiber stacks lazily without overflowing the mark stack" do
    {% if flag?(:patched_libgc) %}
      pending!("needs parallel GC markers") if LibGC.get_parallel == 0
      pending!("GC_DISABLE_GROW_MARK_STACK is set") if LibGC.get_grow_mark_stack == 0
      with_refs_on_parked_stack do
        with_grow_mark_stack(0) do
          mark_stack_overflows { GC.collect }.should be > 0
        end
        mark_stack_overflows { GC.collect }.should eq 0
      end
    {% else %}
      pending!("system libgc, build with make to link cloudamqp/bdwgc") unless ENV["CI"]?
      fail "built without -Dpatched_libgc"
    {% end %}
  end

  [0, 1].each do |grow|
    it "keeps objects referenced only from parked fiber stacks (grow_mark_stack=#{grow})" do
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
      with_grow_mark_stack(grow) do
        4.times do
          GC.collect
          Array.new(fibers) { Array.new(4, -1) }
        end
      end
      fibers.times { done.send nil }
      intact = Array.new(fibers) { results.receive }
      intact.count(false).should eq 0
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

def with_grow_mark_stack(value : Int32, &)
  {% if flag?(:patched_libgc) %}
    previous = LibGC.get_grow_mark_stack
    LibGC.set_grow_mark_stack(value)
    begin
      yield
    ensure
      LibGC.set_grow_mark_stack(previous)
    end
  {% else %}
    yield
  {% end %}
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
