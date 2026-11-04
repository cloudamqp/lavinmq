{% unless flag?(:gc_none) || flag?(:interpreted) %}
  module GC
    # GC_push_all_grow (packaging/libgc/push-all-grow.patch) queues a parked
    # fiber's stack for the parallel markers, where GC_push_all_eager scans it
    # serially on the collecting thread. A stock libgc lacks it: stay eager.
    @@push_all_grow : Proc(Void*, Void*, Nil)? = nil

    def self.push_all_grow? : Bool
      !@@push_all_grow.nil?
    end

    # dlsym can malloc, so this runs at startup and never inside a collection
    def self.resolve_push_all_grow : Nil
      ptr = LibC.dlsym(LibC::RTLD_DEFAULT, "GC_push_all_grow")
      return if ptr.null?
      @@push_all_grow = Proc(Void*, Void*, Nil).new(ptr, Pointer(Void).null)
    end

    def self.push_stack(stack_top, stack_bottom) : Nil
      if push = @@push_all_grow
        push.call(stack_top, stack_bottom)
      else
        LibGC.push_all_eager(stack_top, stack_bottom)
      end
    end
  end

  GC.resolve_push_all_grow
{% end %}
