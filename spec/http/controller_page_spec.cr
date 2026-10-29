require "../spec_helper"

class PageSpecItem
  include LavinMQ::SortableJSON
  class_property built = 0
  property? gone = false

  def initialize(@name : String, @n : Int32)
  end

  def details_tuple
    @@built += 1
    raise "gone" if @gone
    {name: @name, n: @n}
  end
end

class PageSpecController < LavinMQ::HTTP::Controller
  class_property items = Array(LavinMQ::SortableJSON).new

  private def register_routes
    get "/api/page-spec" { |context, _| page(context, self.class.items) }
  end
end

def page_spec_get(controller, query) : JSON::Any
  io = IO::Memory.new
  request = ::HTTP::Request.new("GET", "/api/page-spec?#{query}")
  context = ::HTTP::Server::Context.new(request, ::HTTP::Server::Response.new(io))
  controller.call(context)
  context.response.close
  JSON.parse(::HTTP::Client::Response.from_io(io.rewind).body)
end

describe LavinMQ::HTTP::Controller do
  describe "#page" do
    items = (0...10).map { |i| PageSpecItem.new("item#{i}", 9 - i).as(LavinMQ::SortableJSON) }

    it "builds details only for the items on the page" do
      with_amqp_server do |s|
        PageSpecController.items = items
        PageSpecItem.built = 0
        body = page_spec_get(PageSpecController.new(s), "page=2&page_size=3")
        body["items"].as_a.map(&.["name"]).should eq ["item3", "item4", "item5"]
        body["item_count"].should eq 3
        body["filtered_count"].should eq 10
        body["total_count"].should eq 10
        body["page_count"].should eq 4
        PageSpecItem.built.should eq 3
      end
    end

    it "filters and counts without building details" do
      with_amqp_server do |s|
        PageSpecController.items = items
        PageSpecItem.built = 0
        body = page_spec_get(PageSpecController.new(s), "page=1&page_size=2&name=item1")
        body["items"].as_a.map(&.["name"]).should eq ["item1"]
        body["filtered_count"].should eq 1
        body["total_count"].should eq 10
        PageSpecItem.built.should eq 1
      end
    end

    it "leaves out an item that can't be described, filling its slot and not counting it" do
      with_amqp_server do |s|
        PageSpecController.items = items
        items[4].as(PageSpecItem).gone = true
        begin
          body = page_spec_get(PageSpecController.new(s), "page=2&page_size=3")
          body["items"].as_a.map(&.["name"]).should eq ["item3", "item5", "item6"]
          body["item_count"].should eq 3
          body["filtered_count"].should eq 9
          body["page_count"].should eq 3
        ensure
          items[4].as(PageSpecItem).gone = false
        end
      end
    end

    it "selects columns" do
      with_amqp_server do |s|
        PageSpecController.items = items
        body = page_spec_get(PageSpecController.new(s), "page=1&page_size=1&columns=n,missing")
        body["items"].as_a.first.as_h.keys.should eq ["n"]
      end
    end
  end
end
