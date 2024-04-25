module LavinMQ
  module Reportable
    abstract def __report(reporter : Reporter)

    # Used to generate the __report method for Reportable. Items are a list
    # of instance variables (must be prefiex with @) and getters (no prefix)
    # that will be reported.
    # If the variable/getter is a collection all items will also be reported if
    # possible.
    macro reportables(*items, &block)
      def __report(reporter : Reporter)
        # If a block is given, we include that body first, i.e
        # the block is run before items is reported
        {% if block %}
          begin
            # If the block is defined with a paratmeter, assign reporter to
            # that name
            {% if block.args.size > 0 %}
              {{ block.args.first }} = reporter
            {% end %}
            {{ block.body }}
          end
        {% end %}
        {% for item in items %}
          reporter.report_capacity {{item.stringify}}, {{item.id}}
          reporter.report {{item.id}}
        {% end %}
      end

      def __report_type_name
        {{@type.name.split("::").last.titleize}}
      end
    end
  end

  class Reporter
    def initialize(@io : IO = STDOUT)
      @level = -1
    end

    def report(reportables : Enumerable(Tuple(String, Reportable)))
      reportables.each do |name, reportable|
        report(reportable, header: "#{reportable.__report_type_name} #{name}")
      end
    end

    def report(reportables : Enumerable(Tuple(UInt16, Reportable)))
      reportables.each do |id, reportable|
        report(reportable, header: "#{reportable.__report_type_name} #{id}")
      end
    end

    def report(reportables : Enumerable(Reportable))
      reportables.each &->self.report(Reportable)
    end

    def report(reportable : Reportable, header : String? = nil)
      @level += 1
      if header
        report_raw(header)
        @level += 1
      end
      reportable.__report(self)
      if header
        @level -= 1
      end
      @level -= 1
    end

    def report(_any)
      # nop
    end

    def report_raw(value : String)
      @io << indent << value << '\n'
    end

    def report_capacity(name, obj)
      @io << indent << name << "\tsize=" << obj.size
      if obj.responds_to?(:capacity)
        @io << "\tcapacity=" << obj.capacity
      end
      @io << '\n'
    end

    macro indent
      " "*(@level*2)
    end

    def self.report(s)
      r = self.new
      r.report(s)
    end
  end
end
