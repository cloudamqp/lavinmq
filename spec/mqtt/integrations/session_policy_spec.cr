require "../spec_helper"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers

  describe "MQTT::Session policies" do
    it "enforces a max-length policy by dropping the oldest messages" do
      with_server do |server|
        vhost = server.vhosts["/"]
        session = persistent_session(server, "t")

        defs = {"max-length" => JSON::Any.new(3_i64)} of String => JSON::Any
        vhost.add_policy("ml", "^mqtt\\.", "queues", defs, 10_i8, apply: false)
        vhost.apply_policies
        session.details_tuple[:effective_policy_arguments].should contain("max-length")

        publish_messages(server, "t", 10)
        session.message_count.should eq 3
      end
    end

    it "drops existing messages when a max-length policy is applied" do
      with_server do |server|
        vhost = server.vhosts["/"]
        session = persistent_session(server, "t")

        # Fill the session before any policy exists.
        publish_messages(server, "t", 5)
        session.message_count.should eq 5

        # Applying the policy must trim the backlog down to the limit.
        defs = {"max-length" => JSON::Any.new(2_i64)} of String => JSON::Any
        vhost.add_policy("ml", "^mqtt\\.", "queues", defs, 10_i8, apply: false)
        vhost.apply_policies
        session.message_count.should eq 2
      end
    end

    it "enforces a max-length-bytes policy by capping the stored bytes" do
      with_server do |server|
        vhost = server.vhosts["/"]
        session = persistent_session(server, "t")

        # Measure the stored size of a single (fixed-payload) message.
        publish_messages(server, "t", 1)
        session.message_count.should eq 1
        per_msg_bytes = session.details_tuple[:ready_bytes]

        # Allow at most two messages worth of bytes.
        limit = (per_msg_bytes * 2).to_i64
        defs = {"max-length-bytes" => JSON::Any.new(limit)} of String => JSON::Any
        vhost.add_policy("mlb", "^mqtt\\.", "queues", defs, 10_i8, apply: false)
        vhost.apply_policies
        session.details_tuple[:effective_policy_arguments].should contain("max-length-bytes")

        publish_messages(server, "t", 5)
        session.message_count.should eq 2
        session.details_tuple[:ready_bytes].should eq limit
      end
    end

    it "applies the most restrictive of a policy and operator policy" do
      with_server do |server|
        vhost = server.vhosts["/"]
        session = persistent_session(server, "t")

        vhost.add_policy("ml", "^mqtt\\.", "queues",
          {"max-length" => JSON::Any.new(2_i64)} of String => JSON::Any, 10_i8, apply: false)
        vhost.apply_policies
        publish_messages(server, "t", 5)
        session.message_count.should eq 2

        # A stricter operator policy wins over the normal policy.
        vhost.add_operator_policy("mlop", "^mqtt\\.", "queues",
          {"max-length" => JSON::Any.new(1_i64)} of String => JSON::Any, 10_i8, apply: false)
        vhost.apply_policies
        session.message_count.should eq 1

        # Removing the operator policy restores the normal policy's limit.
        vhost.delete_operator_policy("mlop")
        vhost.apply_policies
        publish_messages(server, "t", 5)
        session.message_count.should eq 2
      end
    end

    # Regression: deleting (or loosening) a policy must lift the limit it set.
    # Session#clear_policy_arguments used to be a no-op, so policy-derived limits
    # were never reset and stayed in effect after the policy was gone.
    it "removes the max-length limit when the policy is deleted" do
      with_server do |server|
        vhost = server.vhosts["/"]
        session = persistent_session(server, "t")

        # Apply max-length=1 and confirm it limits the session.
        defs = {"max-length" => JSON::Any.new(1_i64)} of String => JSON::Any
        vhost.add_policy("ml", "^mqtt\\.", "queues", defs, 10_i8, apply: false)
        vhost.apply_policies
        session.policy.try(&.name).should eq "ml"
        session.details_tuple[:effective_policy_arguments].should contain("max-length")

        publish_messages(server, "t", 3)
        session.message_count.should eq 1

        # Delete the policy: the limit must be cleared.
        vhost.delete_policy("ml")
        vhost.apply_policies
        session.policy.should be_nil
        session.details_tuple[:effective_policy_arguments].should be_empty

        # The one retained message stays; new publishes are no longer capped.
        publish_messages(server, "t", 3)
        session.message_count.should eq 4
      end
    end
  end

  # Create a persistent (clean_session: false) session subscribed to `topic` at
  # QoS 1, then disconnect so published messages queue in the store instead of
  # being delivered. Returns the session once its client is gone, so the deliver
  # loop can't drain the store from under the assertions.
  def self.persistent_session(server, topic)
    with_client_io(server) do |io|
      connect(io, clean_session: false)
      subscribe(io, topic_filters: mk_topic_filters({topic, 1u8}))
      disconnect(io)
    end
    session = server.vhosts["/"].session("mqtt.client_id")
    wait_for { session.consumer_count.zero? }
    session
  end

  # Publish `count` QoS 1 messages to `topic`; each publish blocks on its PubAck,
  # which the broker sends only after routing the message into the session store.
  def self.publish_messages(server, topic, count)
    with_client_io(server) do |pub_io|
      connect(pub_io, client_id: "publisher", clean_session: true)
      count.times { publish(pub_io, topic: topic, qos: 1u8) }
      disconnect(pub_io)
    end
  end
end
