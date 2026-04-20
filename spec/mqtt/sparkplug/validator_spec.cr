require "../spec_helper"

module SparkplugSpecs
  describe LavinMQ::MQTT::Sparkplug::Validator do
    describe "#validate_topic" do
      it "accepts valid NBIRTH topic" do
        topic = "spBv3.0/group1/NBIRTH/node1"
        result = LavinMQ::MQTT::Sparkplug::Validator.validate_topic(topic)
        result.should eq(LavinMQ::MQTT::Sparkplug::MessageType::NBIRTH)
      end

      it "accepts valid NDEATH topic" do
        topic = "spBv3.0/group1/NDEATH/node1"
        result = LavinMQ::MQTT::Sparkplug::Validator.validate_topic(topic)
        result.should eq(LavinMQ::MQTT::Sparkplug::MessageType::NDEATH)
      end

      it "accepts valid DBIRTH topic with device" do
        topic = "spBv3.0/group1/DBIRTH/node1/device1"
        result = LavinMQ::MQTT::Sparkplug::Validator.validate_topic(topic)
        result.should eq(LavinMQ::MQTT::Sparkplug::MessageType::DBIRTH)
      end

      it "accepts valid DDEATH topic with device" do
        topic = "spBv3.0/group1/DDEATH/node1/device1"
        result = LavinMQ::MQTT::Sparkplug::Validator.validate_topic(topic)
        result.should eq(LavinMQ::MQTT::Sparkplug::MessageType::DDEATH)
      end

      it "accepts valid NDATA topic" do
        topic = "spBv3.0/group1/NDATA/node1"
        result = LavinMQ::MQTT::Sparkplug::Validator.validate_topic(topic)
        result.should eq(LavinMQ::MQTT::Sparkplug::MessageType::NDATA)
      end

      it "accepts valid DDATA topic with device" do
        topic = "spBv3.0/group1/DDATA/node1/device1"
        result = LavinMQ::MQTT::Sparkplug::Validator.validate_topic(topic)
        result.should eq(LavinMQ::MQTT::Sparkplug::MessageType::DDATA)
      end

      it "accepts valid NCMD topic" do
        topic = "spBv3.0/group1/NCMD/node1"
        result = LavinMQ::MQTT::Sparkplug::Validator.validate_topic(topic)
        result.should eq(LavinMQ::MQTT::Sparkplug::MessageType::NCMD)
      end

      it "accepts valid DCMD topic with device" do
        topic = "spBv3.0/group1/DCMD/node1/device1"
        result = LavinMQ::MQTT::Sparkplug::Validator.validate_topic(topic)
        result.should eq(LavinMQ::MQTT::Sparkplug::MessageType::DCMD)
      end

      it "accepts valid STATE topic" do
        topic = "spBv3.0/group1/STATE/host1"
        result = LavinMQ::MQTT::Sparkplug::Validator.validate_topic(topic)
        result.should eq(LavinMQ::MQTT::Sparkplug::MessageType::STATE)
      end

      it "accepts identifiers with alphanumeric, dash, underscore, period" do
        topic = "spBv3.0/group-1.2/NBIRTH/node_1.test"
        result = LavinMQ::MQTT::Sparkplug::Validator.validate_topic(topic)
        result.should eq(LavinMQ::MQTT::Sparkplug::MessageType::NBIRTH)
      end

      it "rejects invalid namespace" do
        topic = "spBv2.0/group1/NBIRTH/node1"
        expect_raises(LavinMQ::MQTT::Sparkplug::ValidationError, /Invalid namespace/) do
          LavinMQ::MQTT::Sparkplug::Validator.validate_topic(topic)
        end
      end

      it "rejects invalid message type" do
        topic = "spBv3.0/group1/INVALID/node1"
        expect_raises(LavinMQ::MQTT::Sparkplug::ValidationError, /Unknown message type/) do
          LavinMQ::MQTT::Sparkplug::Validator.validate_topic(topic)
        end
      end

      it "rejects missing group_id" do
        topic = "spBv3.0//NBIRTH/node1"
        expect_raises(LavinMQ::MQTT::Sparkplug::ValidationError, /Missing group_id/) do
          LavinMQ::MQTT::Sparkplug::Validator.validate_topic(topic)
        end
      end

      it "rejects missing edge_node_id" do
        topic = "spBv3.0/group1/NBIRTH/"
        expect_raises(LavinMQ::MQTT::Sparkplug::ValidationError, /Missing edge_node_id/) do
          LavinMQ::MQTT::Sparkplug::Validator.validate_topic(topic)
        end
      end

      it "rejects DBIRTH without device_id" do
        topic = "spBv3.0/group1/DBIRTH/node1"
        expect_raises(LavinMQ::MQTT::Sparkplug::ValidationError, /requires device_id/) do
          LavinMQ::MQTT::Sparkplug::Validator.validate_topic(topic)
        end
      end

      it "rejects DDEATH without device_id" do
        topic = "spBv3.0/group1/DDEATH/node1"
        expect_raises(LavinMQ::MQTT::Sparkplug::ValidationError, /requires device_id/) do
          LavinMQ::MQTT::Sparkplug::Validator.validate_topic(topic)
        end
      end

      it "rejects DDATA without device_id" do
        topic = "spBv3.0/group1/DDATA/node1"
        expect_raises(LavinMQ::MQTT::Sparkplug::ValidationError, /requires device_id/) do
          LavinMQ::MQTT::Sparkplug::Validator.validate_topic(topic)
        end
      end

      it "rejects DCMD without device_id" do
        topic = "spBv3.0/group1/DCMD/node1"
        expect_raises(LavinMQ::MQTT::Sparkplug::ValidationError, /requires device_id/) do
          LavinMQ::MQTT::Sparkplug::Validator.validate_topic(topic)
        end
      end

      it "rejects identifiers with invalid characters" do
        topic = "spBv3.0/group@1/NBIRTH/node1"
        expect_raises(LavinMQ::MQTT::Sparkplug::ValidationError, /contains disallowed characters/) do
          LavinMQ::MQTT::Sparkplug::Validator.validate_topic(topic)
        end
      end

      it "rejects identifiers with spaces" do
        topic = "spBv3.0/group 1/NBIRTH/node1"
        expect_raises(LavinMQ::MQTT::Sparkplug::ValidationError, /contains disallowed characters/) do
          LavinMQ::MQTT::Sparkplug::Validator.validate_topic(topic)
        end
      end

      it "allows certificate subscription topic" do
        topic = "$sparkplug/certificates/#"
        result = LavinMQ::MQTT::Sparkplug::Validator.validate_topic(topic)
        result.should be_nil # Valid but not a message type
      end

      it "returns nil for certificate topic" do
        topic = "$sparkplug/certificates/group1/NBIRTH/node1"
        result = LavinMQ::MQTT::Sparkplug::Validator.validate_topic(topic)
        result.should be_nil
      end
    end

    describe "#certificate_topic?" do
      it "returns true for certificate wildcard topic" do
        topic = "$sparkplug/certificates/#"
        LavinMQ::MQTT::Sparkplug::Validator.certificate_topic?(topic).should be_true
      end

      it "returns true for specific certificate topic" do
        topic = "$sparkplug/certificates/group1/NBIRTH/node1"
        LavinMQ::MQTT::Sparkplug::Validator.certificate_topic?(topic).should be_true
      end

      it "returns false for regular Sparkplug topic" do
        topic = "spBv3.0/group1/NBIRTH/node1"
        LavinMQ::MQTT::Sparkplug::Validator.certificate_topic?(topic).should be_false
      end

      it "returns false for non-Sparkplug topic" do
        topic = "sensor/temperature"
        LavinMQ::MQTT::Sparkplug::Validator.certificate_topic?(topic).should be_false
      end
    end

    describe "#sparkplug_topic?" do
      it "returns true for Sparkplug topics" do
        topic = "spBv3.0/group1/NBIRTH/node1"
        LavinMQ::MQTT::Sparkplug::Validator.sparkplug_topic?(topic).should be_true
      end

      it "returns true for certificate topics" do
        topic = "$sparkplug/certificates/#"
        LavinMQ::MQTT::Sparkplug::Validator.sparkplug_topic?(topic).should be_true
      end

      it "returns false for non-Sparkplug topics" do
        topic = "sensor/temperature"
        LavinMQ::MQTT::Sparkplug::Validator.sparkplug_topic?(topic).should be_false
      end
    end
  end
end
