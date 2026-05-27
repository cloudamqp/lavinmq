require "../spec_helper"

module SparkplugSpecs
  # Collect the topics yielded by expand_certificate_subscription into an array
  def self.expand(filter : String) : Array(String)
    result = [] of String
    LavinMQ::MQTT::Sparkplug::CertificateMapper.expand_certificate_subscription(filter) do |topic|
      result << topic
    end
    result
  end

  describe LavinMQ::MQTT::Sparkplug::CertificateMapper do
    describe "#expand_certificate_subscription" do
      it "expands wildcard to NBIRTH and DBIRTH patterns" do
        result = expand("$sparkplug/certificates/#")
        result.should contain("spBv3.0/+/NBIRTH/+")
        result.should contain("spBv3.0/+/DBIRTH/+/+")
        result.size.should eq(2)
      end

      it "expands group-specific certificate subscription" do
        result = expand("$sparkplug/certificates/group1/#")
        result.should contain("spBv3.0/group1/NBIRTH/+")
        result.should contain("spBv3.0/group1/DBIRTH/+/+")
        result.size.should eq(2)
      end

      it "expands NBIRTH-specific certificate subscription" do
        result = expand("$sparkplug/certificates/group1/NBIRTH/#")
        result.should contain("spBv3.0/group1/NBIRTH/+")
        result.size.should eq(1)
      end

      it "expands specific NBIRTH certificate" do
        result = expand("$sparkplug/certificates/group1/NBIRTH/node1")
        result.should contain("spBv3.0/group1/NBIRTH/node1")
        result.size.should eq(1)
      end

      it "expands DBIRTH-specific certificate subscription" do
        result = expand("$sparkplug/certificates/group1/DBIRTH/#")
        result.should contain("spBv3.0/group1/DBIRTH/+/+")
        result.size.should eq(1)
      end

      it "expands DBIRTH for specific edge node" do
        result = expand("$sparkplug/certificates/group1/DBIRTH/node1/#")
        result.should contain("spBv3.0/group1/DBIRTH/node1/+")
        result.size.should eq(1)
      end

      it "expands specific DBIRTH certificate" do
        result = expand("$sparkplug/certificates/group1/DBIRTH/node1/device1")
        result.should contain("spBv3.0/group1/DBIRTH/node1/device1")
        result.size.should eq(1)
      end

      it "yields nothing for invalid certificate topic with just prefix" do
        expand("$sparkplug/certificates/").should be_empty
      end

      it "yields nothing for incomplete certificate topic" do
        expand("$sparkplug/certificates/group1").should be_empty
      end

      it "yields nothing for incomplete NBIRTH certificate topic" do
        expand("$sparkplug/certificates/group1/NBIRTH").should be_empty
      end

      it "yields nothing for incomplete DBIRTH certificate topic without device" do
        expand("$sparkplug/certificates/group1/DBIRTH/node1").should be_empty
      end

      it "yields nothing for non-BIRTH message types" do
        expand("$sparkplug/certificates/group1/NDATA/#").should be_empty
      end

      it "yields nothing for invalid message type" do
        expand("$sparkplug/certificates/group1/INVALID/#").should be_empty
      end

      it "yields the original filter for non-certificate topics" do
        expand("sensor/temperature").should eq(["sensor/temperature"])
      end
    end

    describe "#certificate_topic?" do
      it "returns true for certificate wildcard topic" do
        topic = "$sparkplug/certificates/#"
        LavinMQ::MQTT::Sparkplug::CertificateMapper.certificate_topic?(topic).should be_true
      end

      it "returns true for specific certificate topic" do
        topic = "$sparkplug/certificates/group1/NBIRTH/node1"
        LavinMQ::MQTT::Sparkplug::CertificateMapper.certificate_topic?(topic).should be_true
      end

      it "returns false for regular Sparkplug topic" do
        topic = "spBv3.0/group1/NBIRTH/node1"
        LavinMQ::MQTT::Sparkplug::CertificateMapper.certificate_topic?(topic).should be_false
      end

      it "returns false for non-Sparkplug topic" do
        topic = "sensor/temperature"
        LavinMQ::MQTT::Sparkplug::CertificateMapper.certificate_topic?(topic).should be_false
      end
    end
  end
end
