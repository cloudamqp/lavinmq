module LavinMQ
  module GlobalCounters
    # global counters
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
      # @global_messages_unroutable_returned_total += vhost.message_details[:message_stats][:return_unroutable]
      # @global_messages_delivered_get_manual_ack_total += vhost.message_details[:message_stats][:get_manual_ack]
      # @global_messages_delivered_get_auto_ack_total += vhost.message_details[:message_stats][:get_auto_ack]
      # @global_messages_get_empty_total += vhost.message_details[:message_stats][:get_empty]
    end
  end
end
