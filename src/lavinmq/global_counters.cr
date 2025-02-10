module LavinMQ
  module GlobalCounters
    # global counters
    property successful_auths = 0
    property failed_auths = 0
    property global_messages_delivered_total = 0
    property global_messages_delivered_consume_manual_ack_total = 0
    property global_messages_delivered_consume_auto_ack_total = 0
    property global_messages_delivered_get_manual_ack_total = 0
    property global_messages_delivered_get_auto_ack_total = 0
    property global_messages_get_empty_total = 0
    property global_messages_redelivered_total = 0
    property global_messages_acknowledged_total = 0
    property global_messages_received_total = 0
    property global_messages_received_confirm_total = 0
    property global_messages_routed_total = 0
    property global_messages_unroutable_dropped_total = 0
    property global_messages_unroutable_returned_total = 0
    property global_messages_confirmed_total = 0
    property global_messages_dead_lettered_maxlen_total = 0
    property global_messages_dead_lettered_expired_total = 0
    property global_messages_dead_lettered_rejected_total = 0
    property global_messages_dead_lettered_delivery_limit_total = 0
    property global_messages_dead_lettered_confirmed_total = 0

    def update_global_counters(vhost)
      @global_messages_delivered_total += vhost.message_details[:message_stats][:deliver]
      @global_messages_redelivered_total += vhost.message_details[:message_stats][:redeliver]
      @global_messages_confirmed_total += vhost.message_details[:message_stats][:confirm]
      @global_messages_acknowledged_total += vhost.message_details[:message_stats][:ack]
      @global_messages_unroutable_returned_total += vhost.message_details[:message_stats][:return_unroutable]
      @global_messages_delivered_get_manual_ack_total += vhost.message_details[:message_stats][:get_manual_ack]
      @global_messages_delivered_get_auto_ack_total += vhost.message_details[:message_stats][:get_auto_ack]
      @global_messages_get_empty_total += vhost.message_details[:message_stats][:get_empty_count]
      # vhost.message_details.message_stats.get
      # vhost.message_details.message_stats.get_no_ack
      # vhost.message_details.message_stats.deliver_get
      # vhost.message_details.message_stats.return_unroutable
      # puts vhost.message_details[:message_stats]
    end
  end
end
