require "sync/shared"

module LavinMQ
  module Observer(EventT)
    abstract def on(event : EventT, data : Object?)
  end

  module Observable(EventT)
    macro included
      {% observers_ivar = ("@__" + EventT.name.stringify.downcase.gsub(/[^a-z_]+/, "_") + "_observers").id %}
      {{ observers_ivar }} : Sync::Shared(Set(LavinMQ::Observer({{ EventT }}))) = Sync::Shared.new(Set(LavinMQ::Observer({{ EventT }})).new)

      def register_observer(observer : LavinMQ::Observer({{ EventT }}))
        {{ observers_ivar }}.lock { |obs| obs.add(observer) }
      end

      def unregister_observer(observer : LavinMQ::Observer({{ EventT }}))
        {{ observers_ivar }}.lock { |obs| obs.delete(observer) }
      end

      def notify_observers(event : {{ EventT }}, data : Object? = nil)
        observers_copy = {{ observers_ivar }}.shared(&.to_a)
        observers_copy.each &.on(event, data)
      end
    end
  end
end
