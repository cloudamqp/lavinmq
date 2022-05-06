module LavinMQ
  VERSION = {{ `git describe --tags 2>/dev/null || shards version`.chomp.stringify.gsub(/^v/, "") }}

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
    Build flags: #{build_flags}
    INFO
end
