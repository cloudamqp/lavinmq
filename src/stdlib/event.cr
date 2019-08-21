struct Crystal::Event
  def delete
    unless LibEvent2.event_del(@event) == 0
      raise "Error deleting event"
    end
  end
end
