require "./spec_helper"

describe AvalancheMQ::HTTP::StaticController do
  it "GET /" do
    response = ::HTTP::Client.get BASE_URL
    response.status_code.should eq 200
  end
end
