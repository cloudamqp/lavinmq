require "./spec_helper"

describe AvalancheMQ::StaticController do
  it "GET /" do
    response = HTTP::Client.get BASE_URL
    response.status_code.should eq 200
  end
end
