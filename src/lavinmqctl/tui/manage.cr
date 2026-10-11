class LavinMQCtl
  class TUI
    # A change to the broker, made in manage mode after confirming *question*
    record Action, label : String, method : String, path : String, question : String, done : String

    # Shown in the footer for a while, like what an action did
    record Notice, text : String, color : Color, at : Time::Instant

    # What an action changes
    enum Target
      Queue
      Connection
      Channel
      Consumer
      Shovel
    end

    PAGE_TARGETS = {
      queues:      Target::Queue,
      connections: Target::Connection,
      channels:    Target::Channel,
      consumers:   Target::Consumer,
      shovels:     Target::Shovel,
    }

    NOTICE_TIME = 8.seconds
    # The menu's keys are 1-9
    MAX_ACTIONS = 9

    @menu : Array(Action)? = nil
    @confirm : Action? = nil
    @notice : Notice? = nil

    private def notice(text : String, color : Color)
      @notice = Notice.new(text, color, Time.instant)
    end

    private def open_menu
      unless @manage
        return notice("Read-only: start lavinmqctl tui with --manage to pause queues, close connections and more", YELLOW)
      end
      actions = menu_actions
      return notice("Nothing to manage here", MUTED_FG) if actions.empty?
      @menu = actions.first(MAX_ACTIONS)
    end

    # A digit picks an action from the menu, then y makes it, any other key
    # cancels. True when the key was for them.
    private def manage_key(event : KeyEvent) : Bool
      if action = @confirm
        @confirm = nil
        if event.key.char? && event.char.in?('y', 'Y')
          perform(action)
        else
          notice("Cancelled, nothing was changed", MUTED_FG)
        end
        return true
      end
      if menu = @menu
        @menu = nil
        index = event.key.char? ? event.char.to_i? : nil
        if index && index >= 1 && (action = menu[index - 1]?)
          @confirm = action
        end
        return true
      end
      false
    end

    # What the selected row of a table is, or the object of a view and the
    # selected row of its section, can have done to it
    private def menu_actions : Array(Action)
      return [] of Action unless state = table_state
      page_target = PAGE_TARGETS[@page]?
      actions = [] of Action
      if view = state.views.last?
        target = case view.kind
                 in .queue?      then Target::Queue
                 in .connection? then Target::Connection
                 in .channel?    then Target::Channel
                 in .fields?     then page_target
                 end
        actions.concat(actions(target, view.item)) if target && !view.gone?
        if (row_target = section(view).manage) && (row = fetched_row(view.rows, view.cursor - view.rows_first))
          actions.concat(actions(row_target, row))
        end
      elsif page_target && @items_page == @page && (row = fetched_row(@items, state.cursor - @items_first))
        actions.concat(actions(page_target, row))
      end
      actions
    end

    # The selected row if it's fetched, the selection can have moved past
    # the rows fetched until they're fetched again
    private def fetched_row(rows : Array(JSON::Any), index : Int32) : JSON::Any?
      rows[index]? if index >= 0
    end

    private def actions(target : Target, item : JSON::Any) : Array(Action)
      case target
      in .queue?      then queue_actions(item)
      in .connection? then connection_actions(item)
      in .channel?    then channel_actions(item)
      in .consumer?   then consumer_actions(item)
      in .shovel?     then shovel_actions(item)
      end
    end

    private def queue_actions(queue : JSON::Any) : Array(Action)
      vhost = Fields.text(queue, "vhost", default: "")
      name = Fields.text(queue, "name", default: "")
      return [] of Action if name.empty?
      path = "/api/queues/#{URI.encode_path_segment(vhost)}/#{URI.encode_path_segment(name)}"
      what = "queue #{name} in vhost #{vhost}"
      state = Fields.text(queue, "state")
      actions = [] of Action
      if state == "paused"
        actions << Action.new("Resume the consumers of queue #{name}", "PUT", "#{path}/resume",
          "Resume the consumers of #{what}?", "Resumed the consumers of #{what}")
      else
        actions << Action.new("Pause the consumers of queue #{name}", "PUT", "#{path}/pause",
          "Pause the consumers of #{what}? They get no messages until it's resumed.", "Paused the consumers of #{what}")
      end
      if state == "closed"
        actions << Action.new("Restart queue #{name}", "PUT", "#{path}/restart", "Restart #{what}?", "Restarted #{what}")
      end
      actions
    end

    private def connection_actions(connection : JSON::Any) : Array(Action)
      name = Fields.text(connection, "name", default: "")
      return [] of Action if name.empty?
      [Action.new("Close connection #{name}", "DELETE", "/api/connections/#{URI.encode_path_segment(name)}",
        "Close connection #{name}? The client may connect again.", "Closed connection #{name}")]
    end

    private def channel_actions(channel : JSON::Any) : Array(Action)
      name = Fields.text(channel, "name", default: "")
      return [] of Action if name.empty?
      [Action.new("Close channel #{name}", "DELETE", "/api/channels/#{URI.encode_path_segment(name)}",
        "Close channel #{name}? Its unacknowledged messages are requeued.", "Closed channel #{name}")]
    end

    private def consumer_actions(consumer : JSON::Any) : Array(Action)
      tag = Fields.text(consumer, "consumer_tag", default: "")
      vhost = Fields.text(consumer, "queue", "vhost", default: "")
      queue = Fields.text(consumer, "queue", "name", default: "")
      connection = Fields.text(consumer, "channel_details", "connection_name", default: "")
      number = Fields.int(consumer, "channel_details", "number")
      return [] of Action if tag.empty? || connection.empty? || number <= 0
      path = "/api/consumers/#{URI.encode_path_segment(vhost)}/#{URI.encode_path_segment(connection)}/#{number}/#{URI.encode_path_segment(tag)}"
      [Action.new("Cancel consumer #{tag}", "DELETE", path,
        "Cancel consumer #{tag} on queue #{queue}? Its unacknowledged messages are requeued.", "Cancelled consumer #{tag}")]
    end

    private def shovel_actions(shovel : JSON::Any) : Array(Action)
      vhost = Fields.text(shovel, "vhost", default: "")
      name = Fields.text(shovel, "name", default: "")
      return [] of Action if name.empty?
      path = "/api/shovels/#{URI.encode_path_segment(vhost)}/#{URI.encode_path_segment(name)}"
      what = "shovel #{name} in vhost #{vhost}"
      case Fields.text(shovel, "state")
      when "Running"
        [Action.new("Pause shovel #{name}", "PUT", "#{path}/pause", "Pause #{what}?", "Paused #{what}")]
      when "Paused", "Aborted"
        [Action.new("Resume shovel #{name}", "PUT", "#{path}/resume", "Resume #{what}?", "Resumed #{what}")]
      else
        [] of Action
      end
    end

    private def perform(action : Action)
      if @closed && (reconnect = @reconnect)
        @client = reconnect.call
        @closed = false
      end
      headers = HTTP::Headers{"X-Reason" => "Closed with lavinmqctl tui"}
      response = @client.exec(action.method, action.path, headers: headers)
      if response.success?
        notice(action.done, GREEN)
      else
        reason = Fields.text(JSON.parse(response.body), "reason", default: "") rescue ""
        notice("Failed: HTTP #{response.status_code} #{reason.presence || response.status}", RED)
      end
    rescue ex
      @client.close
      @closed = true
      notice("Failed: #{ex.message || ex.class.name}", RED)
    ensure
      # Shows what changed right away
      @fetched = ""
    end

    private def draw_menu(menu : Array(Action))
      lines = menu.map_with_index { |action, i| {(i + 1).to_s, action.label} }
      draw_popup("Manage", lines, "Esc closes", BORDER_FG)
    end

    private def draw_confirm(action : Action)
      draw_popup("Confirm", [{"", action.question}, {"", ""}, {"y", "Yes"}, {"", "Any other key cancels"}], "", YELLOW)
    end

    # A panel in the middle of the screen, over what's under it
    private def draw_popup(title : String, lines : Array({String, String}), note : String, border : Color)
      widest = lines.max_of { |(_, text)| Text.width(text) } + 12
      width = { {widest, 40}.max, @width - 4 }.min
      wrapped = lines.flat_map do |(key, text)|
        wrap(text, width - 10).map_with_index { |line, i| {i.zero? ? key : "", line} }
      end
      height = {wrapped.size + 4, @height - 4}.min
      rect = Rect.new((@width - width) // 2, {(@height - height) // 2, 2}.max, width, height)
      fill_rect(rect.x - 1, rect.y - 1, rect.width + 2, rect.height + 2, bg: BG)
      draw_panel(rect, title, note, border: border)
      wrapped.each_with_index do |(key, text), i|
        y = rect.inner_y + 1 + i
        break if y >= rect.bottom
        print_at(rect.inner_x + 2, y, key, GREEN, PANEL_BG, true)
        print_fit(rect.inner_x + 5, y, text, rect.inner_width - 6, TEXT_FG, PANEL_BG)
      end
    end
  end
end
