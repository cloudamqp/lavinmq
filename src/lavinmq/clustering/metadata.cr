module LavinMQ
  module Clustering
    METADATA_FILES         = {".lock", ".clustering_id", ".clustering_password", ".clustering_vr_state"}
    METADATA_FILE_PREFIXES = {".clustering_password.", ".clustering_vr_state."}

    def self.metadata_file?(name : String) : Bool
      METADATA_FILES.includes?(name) || METADATA_FILE_PREFIXES.any? { |prefix| name.starts_with?(prefix) }
    end
  end
end
