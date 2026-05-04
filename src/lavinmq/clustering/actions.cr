require "../mfile"

module LavinMQ
  module Clustering
    # Action structs are no longer used for replication as followers now
    # receive direct socket writes for better performance.
  end
end
