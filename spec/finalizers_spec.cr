require "./spec_helper"
require "./mqtt/spec_helper"
require "digest/sha1"

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

# Allowed to stay registered: the spec's own AMQP client makes a TLS context per
# connection, and a queue's files keep their finalizer after they're closed.
ALLOWED = %w[OpenSSL::SSL::Context::Client File]

module FinalizerSpecs
  extend MqttHelpers

  describe LavinMQ::EVPDigest do
    it "hashes like Digest::SHA1, which named the queue directories" do
      ["", "q", "amq.gen-1", "æøå/#"].each do |name|
        LavinMQ::EVPDigest.sha1_hexdigest(name).should eq Digest::SHA1.hexdigest(name)
      end
    end
  end

  describe "A closed connection" do
    it "leaves no finalizers registered after AMQP with a temporary queue" do
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

    it "leaves no finalizers registered after AMQP over TLS" do
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

    it "leaves no finalizers registered after an MQTT session" do
      with_server do |s|
        FinalizerSpy.record do
          with_client_io(s) do |io|
            connect(io, clean_session: true)
            disconnect(io)
          end
          wait_for { s.connections.empty? }
        end
        (FinalizerSpy.still_registered - ALLOWED).should be_empty
      end
    end
  end
end
