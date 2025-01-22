module LavinMQ
  enum Tag
    Administrator
    Monitoring
    Management
    PolicyMaker
    Impersonator

    def self.parse_list(list : String) : Array(Tag)
      list.split(",").compact_map { |t| Tag.parse?(t.strip) }
    end
  end
end
