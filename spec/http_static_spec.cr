require "./spec_helper"

describe AvalancheMQ::StaticController do
  it "GET /" do
    response = HTTP::Client.get "http://localhost:8080/"
    response.status_code.should eq 200
  end
end
