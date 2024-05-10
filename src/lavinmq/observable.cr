module LavinMQ
  module Observer(EventT)
    abstract def on(event : EventT, data : Object?)
  end

  module Observable(EventT)
    macro included
      {% observers_ivar = ("@__" + EventT.name.stringify.downcase.gsub(/[^a-z_]+/, "_") + "_observers").id %}
      {{observers_ivar}} = Set(LavinMQ::Observer({{EventT}})).new

      def register_observer(observer : LavinMQ::Observer({{EventT}}))
        {{observers_ivar}}.add(observer)
      end

      def unregister_observer(observer : LavinMQ::Observer({{EventT}}))
        {{observers_ivar}}.delete(observer)
      end

      def notify_observers(event : {{EventT}}, data : Object? = nil)
        {{observers_ivar}}.each &.on(event, data)
      end
    end
  end
end
