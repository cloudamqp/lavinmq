require "spec"
require "../src/lavinmq/observable"

module ObservableSpec
  enum FooEvent
    Foo1
    Foo2
  end
  enum BarEvent
    Bar1
    Bar2
  end

  class Observer(T)
    include LavinMQ::Observer(T)

    def initialize(&@callback : (T, Int32) ->)
    end

    def on(event : T, data : Object?)
      @callback.call(event, data.as(Int32))
    end
  end

  class Observable(T)
    include LavinMQ::Observable(T)
  end

  class Observable2(T, S)
    include LavinMQ::Observable(T)
    include LavinMQ::Observable(S)
  end

  describe LavinMQ::Observable do
    describe "with one observable" do
      describe "#register_observer" do
        it "registers observers" do
          observable = Observable(FooEvent).new

          observable.register_observer(obs1 = Observer(FooEvent).new { |_, _| nil })
          observable.register_observer(obs2 = Observer(FooEvent).new { |_, _| nil })

          observable.@__t_observers.should eq Set{obs1, obs2}
        end
      end

      describe "#unregister_observer" do
        it "unregisters observers" do
          observable = Observable(FooEvent).new

          observable.register_observer(obs1 = Observer(FooEvent).new { |_, _| nil })
          observable.register_observer(obs2 = Observer(FooEvent).new { |_, _| nil })
          observable.unregister_observer(obs1)

          observable.@__t_observers.should eq Set{obs2}
        end
      end

      it "notify observers" do
        observable = Observable(FooEvent).new

        events1 = Array({FooEvent, Int32}).new
        events2 = Array({FooEvent, Int32}).new
        observable.register_observer(Observer(FooEvent).new do |event, data|
          events1 << {event, data}
        end)
        observable.register_observer(Observer(FooEvent).new do |event, data|
          events2 << {event, data}
        end)

        observable.notify_observers(FooEvent::Foo1, 1)
        observable.notify_observers(FooEvent::Foo2, 2)

        events1.should eq [{FooEvent::Foo1, 1}, {FooEvent::Foo2, 2}]
        events2.should eq [{FooEvent::Foo1, 1}, {FooEvent::Foo2, 2}]
      end
    end

    describe "with two observable" do
      it "notify right observers" do
        observable = Observable2(FooEvent, BarEvent).new

        events1 = Array({FooEvent, Int32}).new
        events2 = Array({BarEvent, Int32}).new
        observable.register_observer(Observer(FooEvent).new do |event, data|
          events1 << {event, data}
        end)
        observable.register_observer(Observer(BarEvent).new do |event, data|
          events2 << {event, data}
        end)

        observable.notify_observers(FooEvent::Foo1, 1)
        observable.notify_observers(FooEvent::Foo2, 2)

        events1.should eq [{FooEvent::Foo1, 1}, {FooEvent::Foo2, 2}]
        events2.should be_empty
      end
    end
  end
end
