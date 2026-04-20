require "../../../src/lavinmq/amqp/exchange/topic_binding_key"

module LavinMQWasm
  def self.route_topic(bindings : Array({String, String}), routing_key : String) : Array(String)
    matched = [] of String
    parsed_keys = bindings.map { |queue, bk| {queue, LavinMQ::AMQP::TopicBindingKey.new(bk)} }

    if parsed_keys.size == 1
      queue, tbk = parsed_keys.first
      return [queue] if tbk.acts_as_fanout?
    end

    parsed_keys.each do |queue, tbk|
      matched << queue if tbk.matches?(routing_key)
    end
    matched.uniq
  end
end
