require "json"
require "base64"
require "./errors"
require "./request"
require "./queue_url"
require "./brokers"

module LavinMQ
  module SQS
    # One method per SQS action. Each writes the fields of the response object
    # to the JSON builder; the wire protocol layer supplies the enclosing
    # object and turns raised `Error`s into error responses.
    class Actions
      BATCH_ID_PATTERN = /\A[a-zA-Z0-9_-]{1,80}\z/
      MAX_BATCH_SIZE   =   10
      MAX_LIST_RESULTS = 1000

      def initialize(@brokers : Brokers)
      end

      def dispatch(req : Request, json : JSON::Builder) : Nil # ameba:disable Metrics/CyclomaticComplexity
        case req.action
        when "CreateQueue"                  then create_queue(req, json)
        when "GetQueueUrl"                  then get_queue_url(req, json)
        when "ListQueues"                   then list_queues(req, json)
        when "DeleteQueue"                  then delete_queue(req, json)
        when "GetQueueAttributes"           then get_queue_attributes(req, json)
        when "SetQueueAttributes"           then set_queue_attributes(req, json)
        when "PurgeQueue"                   then purge_queue(req, json)
        when "TagQueue"                     then tag_queue(req, json)
        when "UntagQueue"                   then untag_queue(req, json)
        when "ListQueueTags"                then list_queue_tags(req, json)
        when "SendMessage"                  then send_message(req, json)
        when "SendMessageBatch"             then send_message_batch(req, json)
        when "ReceiveMessage"               then receive_message(req, json)
        when "DeleteMessage"                then delete_message(req, json)
        when "DeleteMessageBatch"           then delete_message_batch(req, json)
        when "ChangeMessageVisibility"      then change_message_visibility(req, json)
        when "ChangeMessageVisibilityBatch" then change_message_visibility_batch(req, json)
        when "ListDeadLetterSourceQueues", "StartMessageMoveTask", "ListMessageMoveTasks",
             "CancelMessageMoveTask", "AddPermission", "RemovePermission"
          raise UnsupportedOperation.new("#{req.action} is not supported by LavinMQ.")
        else
          raise InvalidAction.new("The action #{req.action} is not valid for this endpoint.")
        end
      end

      # Queue management

      def create_queue(req, json) : Nil
        name = req.string("QueueName")
        QueueName.validate!(name)
        attributes = req.string_map?("Attributes") || Hash(String, String).new
        QueueAttributes.validate_all!(attributes)
        tags = req.string_map?("tags") || Hash(String, String).new
        if QueueName.fifo?(name) != (attributes["FifoQueue"]? == "true")
          raise InvalidParameterValue.new("The name of a FIFO queue can only include alphanumeric characters, hyphens, or underscores, must end with .fifo suffix and be 1 to 80 in length")
        end
        vhost = req.path_vhost
        broker = broker_for(vhost)
        authorize!(req, vhost, name, :configure)
        broker.create_queue(name, attributes, tags)
        json.field "QueueUrl", QueueUrl.build(req.base_url, vhost, name)
      end

      def get_queue_url(req, json) : Nil
        name = req.string("QueueName")
        vhost = req.string?("QueueOwnerAWSAccountId").try { |s| QueueUrl.vhost_from_segment(s) } || req.path_vhost
        broker = broker_for(vhost)
        authorize!(req, vhost, name, :read)
        broker.queue(name)
        json.field "QueueUrl", QueueUrl.build(req.base_url, vhost, name)
      end

      def list_queues(req, json) : Nil
        vhost = req.path_vhost
        broker = broker_for(vhost)
        raise AccessDenied.new("User #{req.user.name} has no access to vhost #{vhost}") unless req.user.find_permission(vhost)
        prefix = req.string?("QueueNamePrefix")
        max = req.int("MaxResults", 1..MAX_LIST_RESULTS, MAX_LIST_RESULTS)
        after = req.string?("NextToken").try { |t| Base64.decode_string(t) rescue raise InvalidParameterValue.new("Invalid NextToken") }
        names = broker.queue_names(prefix).select! { |n| req.user.can_read?(vhost, n) }
        names.select! { |n| n > after } if after
        page = names.first(max)
        json.field "QueueUrls" do
          json.array { page.each { |n| json.string QueueUrl.build(req.base_url, vhost, n) } }
        end
        if req.has?("MaxResults") && names.size > page.size && (last = page.last?)
          json.field "NextToken", Base64.strict_encode(last)
        end
      end

      def delete_queue(req, json) : Nil
        broker, _vhost, name = resolve(req, :configure)
        broker.delete_queue(name)
      end

      def get_queue_attributes(req, json) : Nil
        broker, vhost, name = resolve(req, :read)
        q = broker.queue(name)
        meta = broker.meta(name)
        wanted = req.strings?("AttributeNames") || Array(String).new(0)
        all = wanted.empty? || wanted.includes?("All")
        wanted.each do |attr|
          next if attr == "All"
          raise InvalidAttributeName.new("Unknown Attribute #{attr}.") unless QueueAttributes.readable?(attr)
        end
        values = meta.effective_attributes
        values["ApproximateNumberOfMessages"] = q.message_count.to_s
        values["ApproximateNumberOfMessagesNotVisible"] = q.unacked_count.to_s
        values["ApproximateNumberOfMessagesDelayed"] = "0"
        values["CreatedTimestamp"] = meta.created_timestamp.to_s
        values["LastModifiedTimestamp"] = meta.last_modified_timestamp.to_s
        values["QueueArn"] = "arn:aws:sqs:#{req.region}:#{QueueUrl.vhost_segment(vhost)}:#{name}"
        json.field "Attributes" do
          json.object do
            values.each do |k, v|
              json.field k, v if all || wanted.includes?(k)
            end
          end
        end
      end

      def set_queue_attributes(req, json) : Nil
        broker, _vhost, name = resolve(req, :configure)
        attributes = req.string_map?("Attributes") || raise MissingParameter.new("The request must contain the parameter Attributes.")
        attributes.each_key do |k|
          raise InvalidAttributeName.new("Unknown Attribute #{k}.") if QueueAttributes::CREATE_ONLY.includes?(k)
        end
        QueueAttributes.validate_all!(attributes)
        broker.set_attributes(name, attributes)
      end

      def purge_queue(req, json) : Nil
        broker, _vhost, name = resolve(req, :read)
        broker.purge(broker.queue(name))
      end

      def tag_queue(req, json) : Nil
        broker, _vhost, name = resolve(req, :configure)
        tags = req.string_map?("Tags") || raise MissingParameter.new("The request must contain the parameter Tags.")
        broker.set_tags(name, tags)
      end

      def untag_queue(req, json) : Nil
        broker, _vhost, name = resolve(req, :configure)
        keys = req.strings?("TagKeys") || raise MissingParameter.new("The request must contain the parameter TagKeys.")
        broker.remove_tags(name, keys)
      end

      def list_queue_tags(req, json) : Nil
        broker, _vhost, name = resolve(req, :read)
        broker.queue(name)
        json.field "Tags" do
          json.object { broker.meta(name).tags.each { |k, v| json.field k, v } }
        end
      end

      # Messages

      def send_message(req, json) : Nil
        broker, _vhost, name = resolve(req, :write)
        q = broker.queue(name)
        meta = broker.meta(name)
        result = send_one(req, req.params, broker, q, meta)
        write_send_result(json, result)
      end

      def send_message_batch(req, json) : Nil
        broker, _vhost, name = resolve(req, :write)
        q = broker.queue(name)
        meta = broker.meta(name)
        entries = batch_entries(req)
        total = entries.sum { |e| e["MessageBody"]?.try(&.as_s?).try(&.bytesize) || 0 }
        if total > meta.maximum_message_size
          raise BatchRequestTooLong.new("Batch requests cannot be longer than #{meta.maximum_message_size} bytes. You have sent #{total} bytes.")
        end
        batch(json, entries) do |entry, sub|
          result = send_one(req, entry, broker, q, meta)
          write_send_result(sub, result)
        end
      end

      def receive_message(req, json) : Nil
        broker, _vhost, name = resolve(req, :read)
        q = broker.queue(name)
        meta = broker.meta(name)
        max = req.int("MaxNumberOfMessages", 1..10, 1)
        wait = req.int("WaitTimeSeconds", 0..20, meta.receive_wait_time)
        visibility = req.int("VisibilityTimeout", 0..43_200, meta.visibility_timeout)
        system_attrs = req.strings?("MessageSystemAttributeNames") || req.strings?("AttributeNames") || Array(String).new(0)
        attr_names = req.strings?("MessageAttributeNames") || Array(String).new(0)
        messages = broker.receive(q, max, wait.seconds, visibility.seconds)
        return if messages.empty?
        json.field "Messages" do
          json.array do
            messages.each { |m| write_received(json, m, system_attrs, attr_names) }
          end
        end
      end

      def delete_message(req, json) : Nil
        broker, _vhost, name = resolve(req, :read)
        handle = req.string("ReceiptHandle")
        broker.delete_message(broker.queue(name), handle)
      end

      def delete_message_batch(req, json) : Nil
        broker, _vhost, name = resolve(req, :read)
        q = broker.queue(name)
        batch(json, batch_entries(req)) do |entry, _sub|
          handle = entry["ReceiptHandle"]?.try(&.as_s?) || raise MissingParameter.new("The request must contain the parameter ReceiptHandle.")
          broker.delete_message(q, handle)
        end
      end

      def change_message_visibility(req, json) : Nil
        broker, _vhost, name = resolve(req, :read)
        handle = req.string("ReceiptHandle")
        timeout = req.int("VisibilityTimeout", 0..43_200)
        broker.change_visibility(broker.queue(name), handle, timeout.seconds)
      end

      def change_message_visibility_batch(req, json) : Nil
        broker, _vhost, name = resolve(req, :read)
        q = broker.queue(name)
        batch(json, batch_entries(req)) do |entry, _sub|
          handle = entry["ReceiptHandle"]?.try(&.as_s?) || raise MissingParameter.new("The request must contain the parameter ReceiptHandle.")
          timeout = entry["VisibilityTimeout"]?.try(&.as_i64?) || raise MissingParameter.new("The request must contain the parameter VisibilityTimeout.")
          unless (0..43_200).includes?(timeout)
            raise InvalidParameterValue.new("Value #{timeout} for parameter VisibilityTimeout is invalid. Reason: Must be between 0 and 43200.")
          end
          broker.change_visibility(q, handle, timeout.seconds)
        end
      end

      # Helpers

      private def broker_for(vhost : String) : Broker
        @brokers[vhost]? || raise QueueDoesNotExist.new
      end

      # Resolves the QueueUrl parameter and checks the permission. Returns
      # {broker, vhost, queue_name}; the queue itself may not exist.
      private def resolve(req : Request, permission : Symbol) : Tuple(Broker, String, String)
        vhost, name = QueueUrl.parse(req.string("QueueUrl"))
        broker = broker_for(vhost)
        authorize!(req, vhost, name, permission)
        {broker, vhost, name}
      end

      private def authorize!(req : Request, vhost : String, name : String, permission : Symbol) : Nil
        user = req.user
        allowed = case permission
                  when :read      then user.can_read?(vhost, name)
                  when :write     then user.can_write?(vhost, name)
                  when :configure then user.can_config?(vhost, name)
                  else                 false
                  end
        return if allowed
        raise AccessDenied.new("User #{user.name} is not authorized to perform #{req.action} on queue #{name}")
      end

      private def send_one(req : Request, params : JSON::Any, broker : Broker, q : AMQP::Queue, meta : QueueMeta) : Broker::SendResult
        body = params["MessageBody"]?.try(&.as_s?) || raise MissingParameter.new("The request must contain the parameter MessageBody.")
        raise InvalidParameterValue.new("The request must contain a non-empty MessageBody.") if body.empty?
        validate_body!(body)
        attrs = MessageAttribute.parse_all(params["MessageAttributes"]?.try(&.as_h?))
        system_attrs = MessageAttribute.parse_all(params["MessageSystemAttributes"]?.try(&.as_h?), "MessageSystemAttributes")
        system_attrs.each do |a|
          raise InvalidParameterValue.new("Message system attribute name '#{a.name}' is invalid.") unless a.name == "AWSTraceHeader"
        end
        size = body.bytesize + attrs.sum(&.bytesize)
        if size > meta.maximum_message_size
          raise InvalidParameterValue.new("One or more parameters are invalid. Reason: Message must be shorter than #{meta.maximum_message_size} bytes.")
        end
        delay_seconds = parse_delay_seconds(params, meta)
        group_id = params["MessageGroupId"]?.try(&.as_s?)
        dedup_id = params["MessageDeduplicationId"]?.try(&.as_s?)
        if meta.fifo? && group_id.nil?
          raise MissingParameter.new("The request must contain the parameter MessageGroupId.")
        end
        broker.send(q, meta, body, attrs, system_attrs, delay_seconds, group_id, dedup_id, req.user.name)
      end

      private def parse_delay_seconds(params : JSON::Any, meta : QueueMeta) : Int32?
        raw = params["DelaySeconds"]?.try(&.raw)
        return if raw.nil?
        delay = case raw
                when Int    then raw.to_i32
                when String then raw.to_i32?
                end
        unless delay && (0..900).includes?(delay)
          raise InvalidParameterValue.new("Value #{raw} for parameter DelaySeconds is invalid. Reason: DelaySeconds must be >= 0 and <= 900.")
        end
        if meta.fifo?
          raise InvalidParameterValue.new("Value #{raw} for parameter DelaySeconds is invalid. Reason: The request include parameter that is not valid for this queue type.")
        end
        delay
      end

      private def write_send_result(json : JSON::Builder, result : Broker::SendResult) : Nil
        json.field "MessageId", result.message_id
        json.field "MD5OfMessageBody", result.md5_of_body
        json.field "MD5OfMessageAttributes", result.md5_of_attributes if result.md5_of_attributes
        json.field "MD5OfMessageSystemAttributes", result.md5_of_system_attributes if result.md5_of_system_attributes
      end

      private def write_received(json : JSON::Builder, m : Broker::ReceivedMessage,
                                 system_attrs : Array(String), attr_names : Array(String)) : Nil
        msg = m.message
        json.object do
          json.field "MessageId", msg.message_id
          json.field "ReceiptHandle", m.receipt_handle
          json.field "MD5OfBody", m.md5_of_body
          json.field "Body", msg.body
          write_system_attributes(json, m, system_attrs) unless system_attrs.empty?
          write_message_attributes(json, msg.attributes, attr_names) unless attr_names.empty? || msg.attributes.empty?
        end
      end

      private def write_system_attributes(json : JSON::Builder, m : Broker::ReceivedMessage, wanted : Array(String)) : Nil
        msg = m.message
        all = wanted.includes?("All")
        json.field "Attributes" do
          json.object do
            system_attribute(json, "SentTimestamp", msg.sent_timestamp.to_s, all, wanted)
            system_attribute(json, "ApproximateReceiveCount", m.receive_count.to_s, all, wanted)
            system_attribute(json, "ApproximateFirstReceiveTimestamp", m.first_receive_ts.to_s, all, wanted)
            system_attribute(json, "SenderId", msg.sender_id, all, wanted)
            system_attribute(json, "MessageGroupId", msg.group_id, all, wanted)
            system_attribute(json, "MessageDeduplicationId", msg.dedup_id, all, wanted)
            system_attribute(json, "AWSTraceHeader", msg.trace_header, all, wanted)
          end
        end
      end

      private def system_attribute(json : JSON::Builder, name : String, value : String?, all : Bool, wanted : Array(String)) : Nil
        return if value.nil?
        json.field name, value if all || wanted.includes?(name)
      end

      private def write_message_attributes(json : JSON::Builder, attributes : Array(MessageAttribute), selectors : Array(String)) : Nil
        selected = attributes.select { |a| MessageAttribute.selected?(a.name, selectors) }
        return if selected.empty?
        json.field "MD5OfMessageAttributes", Checksums.md5_attributes(selected)
        json.field "MessageAttributes" do
          json.object { selected.each { |a| json.field a.name, a } }
        end
      end

      # SQS only allows the XML 1.0 character set in message bodies
      private def validate_body!(body : String) : Nil
        unless body.valid_encoding?
          raise InvalidMessageContents.new("Invalid binary character found in the message body.")
        end
        body.each_char do |c|
          ord = c.ord
          next if ord == 0x9 || ord == 0xA || ord == 0xD
          next if 0x20 <= ord <= 0xD7FF
          next if 0xE000 <= ord <= 0xFFFD
          next if 0x10000 <= ord <= 0x10FFFF
          raise InvalidMessageContents.new("Invalid binary character '#{c.inspect}' was found in the message body, the set of allowed characters is #x9 | #xA | #xD | #x20 to #xD7FF | #xE000 to #xFFFD | #x10000 to #x10FFFF")
        end
      end

      private def batch_entries(req : Request) : Array(JSON::Any)
        entries = req.array?("Entries") || Array(JSON::Any).new(0)
        raise EmptyBatchRequest.new if entries.empty?
        raise TooManyEntriesInBatchRequest.new if entries.size > MAX_BATCH_SIZE
        ids = entries.map do |e|
          id = e["Id"]?.try(&.as_s?) || raise InvalidBatchEntryId.new
          raise InvalidBatchEntryId.new unless id.matches?(BATCH_ID_PATTERN)
          id
        end
        raise BatchEntryIdsNotDistinct.new unless ids.uniq.size == ids.size
        entries
      end

      # Runs the block per entry, collecting Successful/Failed as SQS does. The
      # block writes its extra Successful fields to the builder it is given.
      private def batch(json : JSON::Builder, entries : Array(JSON::Any), & : JSON::Any, JSON::Builder -> Nil) : Nil
        successful = Array(String).new
        failed = Array(Tuple(String, Error)).new
        entries.each do |entry|
          id = entry["Id"].as_s
          begin
            out = String.build do |io|
              JSON.build(io) do |sub|
                sub.object do
                  sub.field "Id", id
                  yield entry, sub
                end
              end
            end
            successful << out
          rescue ex : Error
            failed << {id, ex}
          end
        end
        json.field "Successful" do
          json.array { successful.each { |s| json.raw s } }
        end
        json.field "Failed" do
          json.array do
            failed.each do |id, ex|
              json.object do
                json.field "Id", id
                json.field "SenderFault", ex.sender?
                json.field "Code", ex.code
                json.field "Message", ex.message
              end
            end
          end
        end
      end
    end
  end
end
