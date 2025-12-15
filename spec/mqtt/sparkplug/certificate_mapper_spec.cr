require "../spec_helper"

module SparkplugSpecs
  describe LavinMQ::MQTT::Sparkplug::CertificateMapper do
    describe "#expand_certificate_subscription" do
      it "expands wildcard to NBIRTH and DBIRTH patterns" do
        filter = "$sparkplug/certificates/#"
        result = LavinMQ::MQTT::Sparkplug::CertificateMapper.expand_certificate_subscription(filter)
        result.should contain("spBv3.0/+/NBIRTH/+")
        result.should contain("spBv3.0/+/DBIRTH/+/+")
        result.size.should eq(2)
      end

      it "expands group-specific certificate subscription" do
        filter = "$sparkplug/certificates/group1/#"
        result = LavinMQ::MQTT::Sparkplug::CertificateMapper.expand_certificate_subscription(filter)
        result.should contain("spBv3.0/group1/NBIRTH/+")
        result.should contain("spBv3.0/group1/DBIRTH/+/+")
        result.size.should eq(2)
      end

      it "expands NBIRTH-specific certificate subscription" do
        filter = "$sparkplug/certificates/group1/NBIRTH/#"
        result = LavinMQ::MQTT::Sparkplug::CertificateMapper.expand_certificate_subscription(filter)
        result.should contain("spBv3.0/group1/NBIRTH/+")
        result.size.should eq(1)
      end

      it "expands specific NBIRTH certificate" do
        filter = "$sparkplug/certificates/group1/NBIRTH/node1"
        result = LavinMQ::MQTT::Sparkplug::CertificateMapper.expand_certificate_subscription(filter)
        result.should contain("spBv3.0/group1/NBIRTH/node1")
        result.size.should eq(1)
      end

      it "expands DBIRTH-specific certificate subscription" do
        filter = "$sparkplug/certificates/group1/DBIRTH/#"
        result = LavinMQ::MQTT::Sparkplug::CertificateMapper.expand_certificate_subscription(filter)
        result.should contain("spBv3.0/group1/DBIRTH/+/+")
        result.size.should eq(1)
      end

      it "expands DBIRTH for specific edge node" do
        filter = "$sparkplug/certificates/group1/DBIRTH/node1/#"
        result = LavinMQ::MQTT::Sparkplug::CertificateMapper.expand_certificate_subscription(filter)
        result.should contain("spBv3.0/group1/DBIRTH/node1/+")
        result.size.should eq(1)
      end

      it "expands specific DBIRTH certificate" do
        filter = "$sparkplug/certificates/group1/DBIRTH/node1/device1"
        result = LavinMQ::MQTT::Sparkplug::CertificateMapper.expand_certificate_subscription(filter)
        result.should contain("spBv3.0/group1/DBIRTH/node1/device1")
        result.size.should eq(1)
      end

      it "returns empty for invalid certificate topic with just prefix" do
        filter = "$sparkplug/certificates/"
        result = LavinMQ::MQTT::Sparkplug::CertificateMapper.expand_certificate_subscription(filter)
        result.should be_empty
      end

      it "returns empty for incomplete certificate topic" do
        filter = "$sparkplug/certificates/group1"
        result = LavinMQ::MQTT::Sparkplug::CertificateMapper.expand_certificate_subscription(filter)
        result.should be_empty
      end

      it "returns empty for incomplete NBIRTH certificate topic" do
        filter = "$sparkplug/certificates/group1/NBIRTH"
        result = LavinMQ::MQTT::Sparkplug::CertificateMapper.expand_certificate_subscription(filter)
        result.should be_empty
      end

      it "returns empty for incomplete DBIRTH certificate topic without device" do
        filter = "$sparkplug/certificates/group1/DBIRTH/node1"
        result = LavinMQ::MQTT::Sparkplug::CertificateMapper.expand_certificate_subscription(filter)
        result.should be_empty
      end

      it "returns empty for non-BIRTH message types" do
        filter = "$sparkplug/certificates/group1/NDATA/#"
        result = LavinMQ::MQTT::Sparkplug::CertificateMapper.expand_certificate_subscription(filter)
        result.should be_empty
      end

      it "returns empty for invalid message type" do
        filter = "$sparkplug/certificates/group1/INVALID/#"
        result = LavinMQ::MQTT::Sparkplug::CertificateMapper.expand_certificate_subscription(filter)
        result.should be_empty
      end

      it "returns original filter for non-certificate topics" do
        filter = "sensor/temperature"
        result = LavinMQ::MQTT::Sparkplug::CertificateMapper.expand_certificate_subscription(filter)
        result.should eq([filter])
      end
    end

    describe "#is_certificate_topic?" do
      it "returns true for certificate wildcard topic" do
        topic = "$sparkplug/certificates/#"
        LavinMQ::MQTT::Sparkplug::CertificateMapper.is_certificate_topic?(topic).should be_true
      end

      it "returns true for specific certificate topic" do
        topic = "$sparkplug/certificates/group1/NBIRTH/node1"
        LavinMQ::MQTT::Sparkplug::CertificateMapper.is_certificate_topic?(topic).should be_true
      end

      it "returns false for regular Sparkplug topic" do
        topic = "spBv3.0/group1/NBIRTH/node1"
        LavinMQ::MQTT::Sparkplug::CertificateMapper.is_certificate_topic?(topic).should be_false
      end

      it "returns false for non-Sparkplug topic" do
        topic = "sensor/temperature"
        LavinMQ::MQTT::Sparkplug::CertificateMapper.is_certificate_topic?(topic).should be_false
      end
    end

    describe "#parse_certificate_topic" do
      it "parses full wildcard certificate topic" do
        topic = "$sparkplug/certificates/#"
        result = LavinMQ::MQTT::Sparkplug::CertificateMapper.parse_certificate_topic(topic)
        result.should eq({nil, nil, nil, nil})
      end

      it "parses group-specific certificate topic" do
        topic = "$sparkplug/certificates/group1/#"
        result = LavinMQ::MQTT::Sparkplug::CertificateMapper.parse_certificate_topic(topic)
        result.should eq({"group1", nil, nil, nil})
      end

      it "parses NBIRTH certificate topic" do
        topic = "$sparkplug/certificates/group1/NBIRTH/#"
        result = LavinMQ::MQTT::Sparkplug::CertificateMapper.parse_certificate_topic(topic)
        result.should eq({"group1", "NBIRTH", nil, nil})
      end

      it "parses specific NBIRTH certificate" do
        topic = "$sparkplug/certificates/group1/NBIRTH/node1"
        result = LavinMQ::MQTT::Sparkplug::CertificateMapper.parse_certificate_topic(topic)
        result.should eq({"group1", "NBIRTH", "node1", nil})
      end

      it "parses specific DBIRTH certificate" do
        topic = "$sparkplug/certificates/group1/DBIRTH/node1/device1"
        result = LavinMQ::MQTT::Sparkplug::CertificateMapper.parse_certificate_topic(topic)
        result.should eq({"group1", "DBIRTH", "node1", "device1"})
      end

      it "returns nil for non-certificate topics" do
        topic = "sensor/temperature"
        result = LavinMQ::MQTT::Sparkplug::CertificateMapper.parse_certificate_topic(topic)
        result.should be_nil
      end
    end
  end
end
