require "spec"
require "../../../src/lavinmq/auth/mechanisms/plain"

describe LavinMQ::Auth::Mechanisms::Plain do
  nul = 0.chr
  ci = LavinMQ::ConnectionInfo.local
  mechanism = LavinMQ::Auth::Mechanisms::Plain.new

  it "extracts username and password from the SASL response" do
    # authzid NUL authcid NUL passwd
    mechanism.credentials("#{nul}user#{nul}pass", ci).should eq({"user", "pass"})
  end

  it "handles an empty password" do
    mechanism.credentials("#{nul}user#{nul}", ci).should eq({"user", ""})
  end

  it "raises when the response has no NUL separator" do
    expect_raises(Exception, /Invalid authentication response/) do
      mechanism.credentials("no-separator", ci)
    end
  end
end
