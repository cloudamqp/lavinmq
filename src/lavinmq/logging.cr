require "./logging/*"
require "./segment_position"

module LavinMQ
  Log = ::Log.for "lmq"

  module Logging
    module Loggable
      @log_context : ::Log::Metadata = ::Log::Metadata.empty

      def _set_log_context(new_metadata : ::Log::Metadata)
        @log_context = new_metadata
      end

      def _set_log_context(new_metadata : NamedTuple)
        @log_context = ::Log::Metadata.build(new_metadata)
      end

      def log_context
        @log_context
      end
    end

    # Set "base metadata" for an instance. Should normally be called from the
    # constructor.
    macro context(*args, **kwargs)
      \{% if @type.ancestors.includes?(::LavinMQ::Logging::Loggable) %}
        {% if args.empty? && kwargs.empty? %}
          @log_context
        {% elsif !args.empty? %}
          _set_log_context({{args.splat}})
        {% else %}
          _set_log_context({{{kwargs.double_splat}}})
          #@log_context = ::Log::Metadata.build({{{kwargs.double_splat}}})
      \{% else %}
        {% receiver = if @caller.first
                        @caller.first.receiver
                      else
                        @type
                      end %}
        \{% raise "Can only use `{{(receiver)}}.context` in classes including LavinMQ::Logging::Loggable" %}
      {% end %}
      \{% end %}
    end

    #
    # Add macro methods for each log level to be able to call it like
    # `Log.info "message", a: 1, b: 2`. It will be converted to
    # `Log.info do |emitter| emitter.emit("message", a: 1, b: 2) end`
    #
    # Exception must be passed as a key-value argument, e.g.
    # `Log.info "message", exception: e` which will be converted to
    # `Log.info exception: e do |emitter| emitter.emit("message") end`
    macro log(level, exception = nil, **metadata, &block)
      \{% begin %}
        \{% is_loggable = (@type.ancestors.includes?(::LavinMQ::Logging::Loggable)) %}
        {%
          metadata = if metadata.empty?
                       "NamedTuple.new".id
                     else
                       "{#{metadata.double_splat}}".id
                     end
        %}
        Log.{{level.id}} exception: {{exception}} do |emitter|
              %msg = begin
                      {{ block.body if block.is_a?(Block) }}
                    end
              break if %msg.nil?
            \{% if is_loggable %}
                emitter.emit(%msg, @log_context.extend( {{metadata}} ))
            \{% else %}
                emitter.emit(%msg, ::Log::Metadata.build( {{metadata}} ))
            \{% end %}
            end
      \{% end %}
     end

    {% for level in %w(trace debug info notice warn error fatal) %}
      macro {{level.id}}(msg = nil, **metadata, &block)
        ::LavinMQ::Logging.log({{level}}\{{ ", #{metadata.double_splat}".id unless metadata.empty? }}) do
            {% verbatim do %}
              {% if msg %}
                {{ msg }}
              {% elsif block.is_a?(Block) %}
                {{ block.body }}
              {% end %}
            {% end %}
          end
      end
    {% end %}
  end

  alias L = Logging
end

class Log::Metadata
  struct Value
    def self.new(value : UInt8 | UInt16)
      new(value.to_u32)
    end

    def self.new(value : Int8 | Int16)
      new(value.to_i32)
    end

    def self.new(value)
      new(value.to_s)
    end
  end
end
