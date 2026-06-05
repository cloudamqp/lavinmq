module LavinMQ
  # Anchors Server#uptime to OS process; survives leader transitions.
  PROCESS_START = Time.instant
end
