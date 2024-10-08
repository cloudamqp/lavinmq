module LavinMQ
  {% if flag?(:windows) %}
    VERSION = {{ `git describe --tags`.chomp.stringify.gsub(/^v/, "") }}
  {% else %}
    VERSION = {{ `git describe --tags 2>/dev/null || shards version`.chomp.stringify.gsub(/^v/, "") }}
  {% end %}

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
