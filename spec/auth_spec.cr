require "./spec_helper"

class MockAuthService < LavinMQ::AuthenticationService
  property? should_succeed : Bool
  property last_username : String?
  property last_password : String?

  def initialize(@should_succeed = false)
  end

  def authorize?(username : String, password : String)
    @last_username = username
    @last_password = password

    if @should_succeed
      "allow"
    else
      try_next(username, password)
    end
  end
end

describe LavinMQ::AuthenticationChain do
  describe "#authorize?" do
    it "returns nil when no services are configured" do
      chain = LavinMQ::AuthenticationChain.new
      chain.authorize?("user", "pass").should be_nil
    end

    it "tries services in order until success" do
      # Arrange
      chain = LavinMQ::AuthenticationChain.new
      service1 = MockAuthService.new(should_succeed: false)
      service2 = MockAuthService.new(should_succeed: true)
      service3 = MockAuthService.new(should_succeed: true)

      chain.add_service(service1)
      chain.add_service(service2)
      chain.add_service(service3)

      # Act
      user = chain.authorize?("test_user", "test_pass")

      # Assert
      user.should_not be_nil
      service1.last_username.should eq("test_user")
      service2.last_username.should eq("test_user")
      service3.last_username.should be_nil # Ne devrait pas être appelé
    end

    it "returns nil if all services fail" do
      chain = LavinMQ::AuthenticationChain.new
      service1 = MockAuthService.new(should_succeed: false)
      service2 = MockAuthService.new(should_succeed: false)

      chain.add_service(service1)
      chain.add_service(service2)

      chain.authorize?("user", "pass").should be_nil
    end
  end
end
