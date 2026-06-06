require "../spec_helper"

module SparkplugSpecs
  # Build a TopicParts by parsing a real Sparkplug topic.
  def self.parts(topic : String) : LavinMQ::MQTT::Sparkplug::TopicParts
    LavinMQ::MQTT::Sparkplug::Validator.parse_topic(topic).not_nil!
  end

  describe LavinMQ::MQTT::Sparkplug::CertificateMapper do
    describe "#certificate_topic_for" do
      it "builds the NBIRTH certificate topic including the namespace segment" do
        topic = LavinMQ::MQTT::Sparkplug::CertificateMapper.certificate_topic_for(
          parts("spBv1.0/G1/NBIRTH/E1"))
        topic.should eq("$sparkplug/certificates/spBv1.0/G1/NBIRTH/E1")
      end

      it "builds the DBIRTH certificate topic including the device_id" do
        topic = LavinMQ::MQTT::Sparkplug::CertificateMapper.certificate_topic_for(
          parts("spBv1.0/G1/DBIRTH/E1/D1"))
        topic.should eq("$sparkplug/certificates/spBv1.0/G1/DBIRTH/E1/D1")
      end
    end

    describe "#certificate_topic?" do
      it "returns true for certificate wildcard topic" do
        topic = "$sparkplug/certificates/#"
        LavinMQ::MQTT::Sparkplug::CertificateMapper.certificate_topic?(topic).should be_true
      end

      it "returns true for specific certificate topic" do
        topic = "$sparkplug/certificates/spBv1.0/group1/NBIRTH/node1"
        LavinMQ::MQTT::Sparkplug::CertificateMapper.certificate_topic?(topic).should be_true
      end

      it "returns false for regular Sparkplug topic" do
        topic = "spBv1.0/group1/NBIRTH/node1"
        LavinMQ::MQTT::Sparkplug::CertificateMapper.certificate_topic?(topic).should be_false
      end

      it "returns false for non-Sparkplug topic" do
        topic = "sensor/temperature"
        LavinMQ::MQTT::Sparkplug::CertificateMapper.certificate_topic?(topic).should be_false
      end
    end
  end
end
