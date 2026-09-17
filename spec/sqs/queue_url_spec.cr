require "./spec_helper"

describe LavinMQ::SQS::QueueUrl do
  it "uses the fake account id for the default vhost" do
    url = LavinMQ::SQS::QueueUrl.build("http://localhost:9324", "/", "orders")
    url.should eq "http://localhost:9324/000000000000/orders"
    LavinMQ::SQS::QueueUrl.parse(url).should eq({"/", "orders"})
  end

  it "encodes other vhost names as a path segment" do
    url = LavinMQ::SQS::QueueUrl.build("https://mq.example.com", "tenant a/b", "orders.fifo")
    url.should eq "https://mq.example.com/tenant%20a%2Fb/orders.fifo"
    LavinMQ::SQS::QueueUrl.parse(url).should eq({"tenant a/b", "orders.fifo"})
  end

  it "accepts any 12 digit account id as the default vhost" do
    LavinMQ::SQS::QueueUrl.parse("http://sqs:9324/123456789012/q").should eq({"/", "q"})
  end

  it "rejects malformed urls with InvalidAddress" do
    expect_raises(LavinMQ::SQS::InvalidAddress) { LavinMQ::SQS::QueueUrl.parse("http://sqs:9324/q") }
    expect_raises(LavinMQ::SQS::InvalidAddress) { LavinMQ::SQS::QueueUrl.parse("not a url") }
    expect_raises(LavinMQ::SQS::InvalidAddress) { LavinMQ::SQS::QueueUrl.parse("http://sqs:9324/a/b/c") }
  end

  it "selects the vhost from the request path" do
    LavinMQ::SQS::QueueUrl.vhost_from_path("/").should eq "/"
    LavinMQ::SQS::QueueUrl.vhost_from_path("").should eq "/"
    LavinMQ::SQS::QueueUrl.vhost_from_path("/000000000000").should eq "/"
    LavinMQ::SQS::QueueUrl.vhost_from_path("/myvhost/").should eq "myvhost"
  end
end

describe LavinMQ::SQS::QueueName do
  it "validates standard and fifo names" do
    LavinMQ::SQS::QueueName.valid?("orders-1_a").should be_true
    LavinMQ::SQS::QueueName.valid?("orders.fifo").should be_true
    LavinMQ::SQS::QueueName.valid?("a" * 80).should be_true
    LavinMQ::SQS::QueueName.valid?("a" * 81).should be_false
    LavinMQ::SQS::QueueName.valid?("orders.queue").should be_false
    LavinMQ::SQS::QueueName.valid?("").should be_false
    LavinMQ::SQS::QueueName.valid?("has space").should be_false
  end
end
