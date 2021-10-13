module AvalancheMQ
  VERSION = {{ `git describe --tags 2>/dev/null || shards version`.chomp.stringify.gsub(/^v/, "") }}

  macro build_flags
    flags = [] of String
    {% if flag?(:release) %}
    flags << "--release"
    {% end %}
    {% if flag?(:debug) %}
    flags << "--debug"
    {% else %}
    flags << "--no-debug"
    {% end %}
    flags
  end

  BUILD_INFO = <<-INFO
    AvalancheMQ #{VERSION}
    #{Crystal::DESCRIPTION.lines.reject(&.empty?).join("\n")}
    Build flags: #{build_flags.join(" ")}
    INFO
end
