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

  module TagListConverter
    def self.from_json(pull : JSON::PullParser) : Array(Tag)
      list = pull.read_string
      list.split(",").compact_map { |t| Tag.parse?(t.strip) }
    end
  end
end
