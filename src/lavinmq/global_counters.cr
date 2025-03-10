module LavinMQ
  module GlobalCounters
    property successful_auths = 0
    property failed_auths = 0
    property global_messages_delivered_total = 0
    property global_messages_redelivered_total = 0
    property global_messages_acknowledged_total = 0
    property global_messages_confirmed_total = 0

    def update_global_counters(vhost)
      @global_messages_delivered_total += vhost.message_details[:message_stats][:deliver]
      @global_messages_redelivered_total += vhost.message_details[:message_stats][:redeliver]
      @global_messages_confirmed_total += vhost.message_details[:message_stats][:confirm]
      @global_messages_acknowledged_total += vhost.message_details[:message_stats][:ack]
    end
  end
end
