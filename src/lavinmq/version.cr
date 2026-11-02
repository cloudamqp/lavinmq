{% unless flag?(:gc_none) %}
  lib LibGC
    fun get_version = GC_get_version : LibC::UInt
  end
{% end %}

module LavinMQ
  VERSION = {{ `[ -n "$version" ] && echo "$version" || git describe --tags 2>/dev/null || shards version`.chomp.stringify.gsub(/^v/, "") }}

  # Version of the Boehm GC linked at runtime (bdwgc encodes it as major<<16 | minor<<8 | micro)
  GC_VERSION = {% if flag?(:gc_none) %}
                 "none"
               {% else %}
                 begin
                   v = LibGC.get_version
                   "#{v >> 16}.#{(v >> 8) & 0xff}.#{v & 0xff}"
                 end
               {% end %}

  # Patches applied to bdwgc, can't be detected at runtime so the build image stamps them in, e.g. GC_PATCHES="fix-foo,fix-bar"
  GC_PATCHES = {{ env("GC_PATCHES") || "" }}

  macro build_flags
    String.build do |flags|
      {% if flag?(:release) %}
        flags << "--release "
      {% end %}
      {% if flag?(:debug) %}
        flags << "--debug"
      {% else %}
        flags << "--no-debug"
      {% end %}
    end
  end

  BUILD_INFO = <<-INFO
    LavinMQ #{VERSION}
    #{Crystal::DESCRIPTION.lines.reject(&.empty?).join("\n")}
    GC: #{GC_VERSION}#{" (patches: #{GC_PATCHES})" unless GC_PATCHES.empty?}
    Build flags: #{build_flags}
    INFO
end
