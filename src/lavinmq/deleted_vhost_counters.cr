module LavinMQ
  module DeletedVhostCounters
    property deleted_vhost_messages_delivered_total = 0
    property deleted_vhost_messages_redelivered_total = 0
    property deleted_vhost_messages_acknowledged_total = 0
    property deleted_vhost_messages_confirmed_total = 0

    def update_deleted_vhost_counters(vhost)
      @deleted_vhost_messages_delivered_total += vhost.message_details[:message_stats][:deliver]
      @deleted_vhost_messages_redelivered_total += vhost.message_details[:message_stats][:redeliver]
      @deleted_vhost_messages_confirmed_total += vhost.message_details[:message_stats][:confirm]
      @deleted_vhost_messages_acknowledged_total += vhost.message_details[:message_stats][:ack]
    end
  end
end
