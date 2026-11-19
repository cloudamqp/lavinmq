require "./spec_helper"
require "./mqtt/spec_helper"

# Records objects that register a GC finalizer, to check which of them are still
# registered later. Closed connections should leave none behind: dead finalizable
# objects make Boehm force a full collection whenever its finalizer table would grow.
module FinalizerSpy
  @@recording = false
  @@objects = [] of {Void*, String}

  # Runs the block once untracked, so things initialized on first use (regex
  # constants and the like) don't count, then again recording.
  def self.record(&)
    yield
    @@objects.clear
    @@recording = true
    yield
  ensure
    @@recording = false
  end

  def self.add(object : Reference) : Nil
    @@objects << {object.as(Void*), object.class.name} if @@recording
  end

  # Class names of recorded objects whose finalizer is still registered
  def self.still_registered : Array(String)
    @@objects.compact_map do |object, name|
      fn = Pointer(Void).null
      data = Pointer(Void).null
      none = LibGC::Finalizer.new(Pointer(Void).null, Pointer(Void).null)
      LibGC.register_finalizer_ignore_self(object, none, nil, pointerof(fn).as(LibGC::Finalizer*), pointerof(data))
      next if fn.null?
      LibGC.register_finalizer_ignore_self(object, LibGC::Finalizer.new(fn, Pointer(Void).null), data, nil, nil)
      name
    end
  end
end

module GC
  private def self.add_finalizer_impl(object : T) forall T
    FinalizerSpy.add(object)
    previous_def
  end
end

# The spec's own AMQP client makes a TLS context per connection
ALLOWED = %w[OpenSSL::SSL::Context::Client]

module FinalizerSpecs
  extend MqttHelpers

  describe "finalizers left registered" do
    it "has none after an AMQP connection with a temporary queue" do
      with_amqp_server do |s|
        FinalizerSpy.record do
          with_channel(s) do |ch|
            q = ch.queue("", exclusive: true)
            q.publish_confirm "m"
            q.get(no_ack: true)
          end
          wait_for { s.connections.empty? && s.vhosts["/"].queues.empty? }
        end
        (FinalizerSpy.still_registered - ALLOWED).should be_empty
      end
    end

    it "has none after an AMQP connection over TLS" do
      with_amqp_server(tls: true) do |s|
        FinalizerSpy.record do
          with_channel(s, port: amqp_tls_port(s), tls: true, verify_mode: OpenSSL::SSL::VerifyMode::NONE) do |ch|
            ch.queue("", exclusive: true).publish_confirm "m"
          end
          wait_for { s.connections.empty? && s.vhosts["/"].queues.empty? }
        end
        (FinalizerSpy.still_registered - ALLOWED).should be_empty
      end
    end

    it "has none after a durable queue is declared, used and deleted" do
      with_amqp_server do |s|
        FinalizerSpy.record do
          with_channel(s) do |ch|
            q = ch.queue("finalizers-durable", durable: true)
            q.publish_confirm "m"
            q.get(no_ack: true)
            q.delete
          end
          wait_for { s.connections.empty? }
        end
        FinalizerSpy.still_registered.should be_empty
      end
    end

    it "has none after segments roll over and are purged" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          q = ch.queue("finalizers-segments", durable: true)
          body = "x" * (3 * 1024 * 1024)
          FinalizerSpy.record do
            4.times { q.publish_confirm body }
            q.purge
          end
          FinalizerSpy.still_registered.should be_empty
        end
      end
    end

    it "has none after an MQTT clean session" do
      with_server do |s|
        FinalizerSpy.record do
          with_client_io(s) do |io|
            connect(io, clean_session: true)
            disconnect(io)
          end
          wait_for { s.connections.empty? }
        end
        FinalizerSpy.still_registered.should be_empty
      end
    end

    it "has none after an MQTT persistent session with a retained publish" do
      with_server do |s|
        FinalizerSpy.record do
          with_client_io(s) do |io|
            connect(io, clean_session: false, client_id: "finalizers")
            publish(io, topic: "finalizers/t", payload: "p".to_slice, qos: 1u8, retain: true)
            disconnect(io)
          end
          wait_for { s.connections.empty? }
        end
        FinalizerSpy.still_registered.should be_empty
      end
    end

    it "has none after HTTP API requests" do
      with_http_server do |http, _|
        FinalizerSpy.record do
          http.get("/api/overview").status_code.should eq 200
          http.get("/api/queues").status_code.should eq 200
        end
        FinalizerSpy.still_registered.should be_empty
      end
    end
  end
end
