{% unless flag?(:gc_none) || flag?(:interpreted) %}
  module GC
    PATCHED = {{ flag?(:patched_libgc) }}

    @@grow_mark_stack = false

    def self.grow_mark_stack? : Bool
      @@grow_mark_stack
    end

    {% if flag?(:patched_libgc) %}
      def self.grow_mark_stack=(value : Bool) : Bool
        LibGC.set_grow_mark_stack(value ? 1 : 0)
        @@grow_mark_stack = value
      end

      def self.push_stack(stack_top, stack_bottom) : Nil
        if @@grow_mark_stack
          LibGC.push_all_grow(stack_top, stack_bottom)
        else
          LibGC.push_all_eager(stack_top, stack_bottom)
        end
      end
    {% else %}
      def self.grow_mark_stack=(value : Bool) : Bool
        raise ArgumentError.new("libgc isn't patched, build with make") if value
        @@grow_mark_stack = value
      end
    {% end %}
  end

  {% if flag?(:patched_libgc) %}
    lib LibGC
      fun set_grow_mark_stack = GC_set_grow_mark_stack(value : Int)
      fun get_grow_mark_stack = GC_get_grow_mark_stack : Int
      fun push_all_grow = GC_push_all_grow(bottom : Void*, top : Void*)
    end
  {% end %}
{% end %}
