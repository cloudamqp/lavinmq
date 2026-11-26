{% if flag?(:patched_libgc) && !flag?(:gc_none) && !flag?(:interpreted) %}
  module GC
    LIBGC_VERSION = {{ env("LIBGC_VERSION") || "unknown" }}

    def self.push_stack(stack_top, stack_bottom) : Nil
      LibGC.push_all_grow(stack_top, stack_bottom)
    end
  end

  lib LibGC
    fun push_all_grow = GC_push_all_grow(bottom : Void*, top : Void*)
    fun get_grow_mark_stack = GC_get_grow_mark_stack : Int
  end
{% end %}

module GC
  # Drops the object's finalizer. Dead finalizable objects make Boehm force
  # collections, so an object that has released its resources should drop it.
  def self.remove_finalizer(object : Reference) : Nil
    {% unless flag?(:gc_none) || flag?(:interpreted) %}
      LibGC.register_finalizer_ignore_self(object.as(Void*),
        LibGC::Finalizer.new(Pointer(Void).null, Pointer(Void).null), nil, nil, nil)
    {% end %}
  end
end
