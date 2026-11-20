{% unless flag?(:gc_none) %}
  require "socket"

  class Socket
    private def unbuffered_close : Nil
      return if @closed

      begin
        previous_def
      ensure
        # `finalize` is a no-op once closed; dropping it lets the GC free us a cycle sooner
        LibGC.register_finalizer_ignore_self(as(Void*),
          LibGC::Finalizer.new(Pointer(Void).null, Pointer(Void).null), nil, nil, nil)
      end
    end
  end
{% end %}
