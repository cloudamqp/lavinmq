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
