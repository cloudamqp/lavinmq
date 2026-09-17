module LavinMQ
  module SQS
    # Base class for every error reported to an SQS client.
    #
    # `code` is the modern error name, sent as the JSON `__type`
    # (`com.amazonaws.sqs#<code>`). `query_code` is the legacy Query protocol
    # code that SDKs still map exception classes and retry behaviour on; it is
    # sent in the `x-amzn-query-error` header.
    class Error < Exception
      getter code : String
      getter query_code : String
      getter status : Int32
      getter? sender : Bool

      def initialize(@code : String, message : String, @query_code : String = @code,
                     @status : Int32 = 400, @sender : Bool = true)
        super(message)
      end
    end

    macro define_error(name, code, query_code = nil, status = 400, sender = true, default_message = "")
      class {{ name }} < Error
        def initialize(message : String = {{ default_message }})
          super({{ code }}, message, {{ query_code || code }}, {{ status }}, {{ sender }})
        end
      end
    end

    define_error InvalidAction, "InvalidAction", default_message: "The action or operation requested is invalid."
    define_error InvalidParameterValue, "InvalidParameterValue"
    define_error MissingParameter, "MissingParameter"
    define_error InvalidAttributeName, "InvalidAttributeName"
    define_error InvalidAttributeValue, "InvalidAttributeValue"
    define_error InvalidAddress, "InvalidAddress", status: 404, default_message: "The address is not valid for this endpoint."
    define_error InvalidMessageContents, "InvalidMessageContents"
    define_error ReceiptHandleIsInvalid, "ReceiptHandleIsInvalid"
    define_error QueueDoesNotExist, "QueueDoesNotExist", "AWS.SimpleQueueService.NonExistentQueue",
      default_message: "The specified queue does not exist."
    define_error QueueNameExists, "QueueNameExists", "QueueAlreadyExists",
      default_message: "A queue already exists with the same name and a different value for an attribute."
    define_error OverLimit, "OverLimit", status: 403
    define_error PurgeQueueInProgress, "PurgeQueueInProgress", "AWS.SimpleQueueService.PurgeQueueInProgress", status: 403,
      default_message: "Only one PurgeQueue operation on a queue is allowed every 60 seconds."
    define_error EmptyBatchRequest, "EmptyBatchRequest", "AWS.SimpleQueueService.EmptyBatchRequest",
      default_message: "There should be at least one entry in the batch request."
    define_error TooManyEntriesInBatchRequest, "TooManyEntriesInBatchRequest", "AWS.SimpleQueueService.TooManyEntriesInBatchRequest",
      default_message: "Maximum number of entries per request are 10."
    define_error BatchEntryIdsNotDistinct, "BatchEntryIdsNotDistinct", "AWS.SimpleQueueService.BatchEntryIdsNotDistinct",
      default_message: "Id of a batch entry in a batch request is not unique."
    define_error InvalidBatchEntryId, "InvalidBatchEntryId", "AWS.SimpleQueueService.InvalidBatchEntryId",
      default_message: "A batch entry id can only contain alphanumeric characters, hyphens and underscores. It can be at most 80 letters long."
    define_error BatchRequestTooLong, "BatchRequestTooLong", "AWS.SimpleQueueService.BatchRequestTooLong"
    define_error UnsupportedOperation, "UnsupportedOperation", "AWS.SimpleQueueService.UnsupportedOperation"
    define_error AccessDenied, "AccessDeniedException", "AccessDenied", status: 403
    define_error MissingAuthenticationToken, "MissingAuthenticationToken", status: 403,
      default_message: "Request is missing Authentication Token"
    define_error InvalidClientTokenId, "InvalidClientTokenId", status: 403,
      default_message: "The security token included in the request is invalid."
    define_error SerializationException, "SerializationException"
    define_error InternalError, "InternalError", status: 500, sender: false,
      default_message: "An internal error occurred."
  end
end
