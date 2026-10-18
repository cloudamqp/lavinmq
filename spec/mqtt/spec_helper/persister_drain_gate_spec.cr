require "../../spec_helper"

# Lets a spec hold the publish confirm loop's drain, so what is sent before
# the data is durable can be observed. One reopen for the whole suite: a
# second `previous_def` chain would receive from the gate twice per drain.
class LavinMQ::Persister
  class_property drain_gate : ::Channel(Nil)? = nil

  private def drain : Nil
    @@drain_gate.try &.receive?
    previous_def
  end
end

module MqttSpecs
  def self.with_drain_held(&)
    gate = ::Channel(Nil).new
    LavinMQ::Persister.drain_gate = gate
    begin
      yield gate
    ensure
      LavinMQ::Persister.drain_gate = nil
      gate.close
    end
  end

  def self.release_drain(gate) : Nil
    LavinMQ::Persister.drain_gate = nil
    gate.close
  end

  # Lets exactly one drain run. The drain is on its own thread, so callers
  # wait for its effect rather than for this to return.
  def self.step_drain(gate) : Nil
    gate.send(nil)
  end
end
