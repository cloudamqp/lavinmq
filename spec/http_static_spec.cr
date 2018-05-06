require "./spec_helper"

describe AvalancheMQ::StaticController do
  it "GET /" do
    s, h = create_servers
    listen(h)
    response = HTTP::Client.get "http://localhost:8080/"
    response.status_code.should eq 200
  ensure
    close(h, s)
  end
end
