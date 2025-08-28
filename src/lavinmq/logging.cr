require "./logging/*"

module LavinMQ
  Log = ::Log.for "lmq"

  module Logging
    module Loggable
      @log_metadata : ::Log::Metadata = ::Log::Metadata.empty
    end

    macro context(**values)
      {% unless values.empty? %}
        \{% if @type.ancestors.includes?(::LavinMQ::Logging::Loggable) %}
          @log_metadata = ::Log::Metadata.build({{{values.double_splat}}})
        \{% else %}
          \{% raise "Can only use `context` macro in classes/modules including LavinMQ::Logging::Loggable" %}
        \{% end %}
      {% end %}
    end

    #
    # Add macro methods for each log level to be able to call it like
    # `Log.info "message", a: 1, b: 2`. It will be converted to
    # `Log.info do |emitter| emitter.emit("message", a: 1, b: 2) end`
    #
    # Exception must be passed as a key-value argument, e.g.
    # `Log.info "message", exception: e` which will be converted to
    # `Log.info exception: e do |emitter| emitter.emit("message") end`
    #
    macro log(level, msg, exception = nil, **metadata)
      \{% begin %}
        {% level = level.id %}
        \{% is_loggable = (@type.ancestors.includes?(::LavinMQ::Logging::Loggable)) %}
        {% if metadata.empty? %}
          \{% if is_loggable %}
            Log.{{level}} exception: {{exception}} do |emitter|
              emitter.emit({{msg}}, @log_metadata)
            end
          \{% else %}
            Log.{{level}} exception: {{exception}} do |emitter|
              emitter.emit({{msg}})
            end
          \{% end %}
        {% else %}
          \{% if is_loggable %}
            Log.{{level}} exception: {{exception}} do |emitter|
              emitter.emit({{msg}}, @log_metadata.extend({ {{metadata.double_splat}} }))
            end
          \{% else %}
            Log.{{level}} exception: {{exception}} do |emitter|
              emitter.emit({{msg}}, {{metadata.double_splat}})
            end
          \{% end %}
        {% end %}
      \{% end %}
    end

    {% for level in %w(trace debug info notice warn error fatal) %}
      macro {{level.id}}(msg, **metadata)
        \{% if metadata.empty? %}
          ::LavinMQ::Logging.log {{level}}, \{{msg}}
        \{% else %}
          ::LavinMQ::Logging.log {{level}}, \{{msg}}, \{{metadata.double_splat}}
        \{% end %}
      end
    {% end %}
  end

  alias L = Logging
end
