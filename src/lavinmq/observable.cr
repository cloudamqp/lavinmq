module LavinMQ
  module Observer(EventT)
    abstract def on(event : EventT, data : Object?)
  end

  module Observable(EventT)
    macro included
      {% ivar_name = ("@__" + EventT.name.stringify.downcase.gsub(/[^a-z_]+/, "_") + "_observers").id %}
      {{ivar_name}} = Set(LavinMQ::Observer({{EventT}})).new

      def register_observer(observer : LavinMQ::Observer({{EventT}}))
        {{ivar_name}}.add(observer)
      end

      def unregister_observer(observer : LavinMQ::Observer({{EventT}}))
        {{ivar_name}}.delete(observer)
      end

      def notify_observers(event : {{EventT}}, data : Object? = nil)
        {{ivar_name}}.each &.on(event, data)
      end
    end
  end
end
