require "./logging/*"
require "log/metadata"

# Add some methods to support more data types in Log::Metadata
class Log::Metadata
  # Override some stuff in Metadata to get the same behaviour
  # as in https://github.com/crystal-lang/crystal/pull/16098
  #
  # @overridden_size will be used as @parent_size in the PR
  #
  def setup
    previous_def
    @overridden_size = 0
    parent_size = (@parent.try(&.max_total_size) || 0)
    @max_total_size = @size + parent_size
  end

  protected def defrag
    parent = @parent
    return if parent.nil?

    ptr_entries = pointerof(@first)
    next_free_entry = ptr_entries + @size
    total_size = @size

    # Copy parent entries that ain't overwritten
    parent_size = 0
    parent.each do |(key, value)|
      overwritten = false
      @size.times do |i|
        if ptr_entries[i][:key] == key
          overwritten = true
          break
        end
      end
      next if overwritten
      next_free_entry.value = {key: key, value: value}
      parent_size += 1
      next_free_entry += 1
      total_size += 1
    end

    @size = total_size
    @max_total_size = total_size
    @overridden_size = parent_size
    @parent = nil
  end

  def each(& : {Symbol, Value} ->)
    defrag
    ptr_entries = pointerof(@first)
    parent_size = @overridden_size
    local_size = @size - parent_size

    parent_size.times do |i|
      entry = ptr_entries[i + local_size]
      yield({entry[:key], entry[:value]})
    end

    local_size.times do |i|
      entry = ptr_entries[i]
      yield({entry[:key], entry[:value]})
    end
  end

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

module LavinMQ
  Log = ::Log.for "lmq"

  module Logging
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
    macro log(level_, exception = nil, **metadata, &block)
      \{% begin %}
        \{% is_loggable = (@type.ancestors.includes?(::LavinMQ::Logging::Loggable)) %}
        {%
          metadata = if metadata.empty?
                       "NamedTuple.new".id
                     else
                       "{#{metadata.double_splat}}".id
                     end
        %}
        Log.{{level_.id}} exception: {{exception}} do |emitter|
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
        ::LavinMQ::Logging.log(:{{level.id}}\{{ ", #{metadata.double_splat}".id unless metadata.empty? }}) do
          {% verbatim do %}
            {% if msg %}
              {{ msg }}
            {% elsif block.is_a?(Block) %}
              {{ block.body }}
            {% end %}
          {% end %}\
        end
      end
    {% end %}
  end

  alias L = Logging
end
