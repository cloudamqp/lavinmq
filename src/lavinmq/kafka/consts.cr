module LavinMQ
  module Kafka
    # Kafka API Keys
    module ApiKey
      Produce     =  0_i16
      Fetch       =  1_i16
      Metadata    =  3_i16
      ApiVersions = 18_i16
    end

    # Kafka Error Codes
    module ErrorCode
      NONE                          =  0_i16
      UNKNOWN_SERVER_ERROR          = -1_i16
      OFFSET_OUT_OF_RANGE           =  1_i16
      CORRUPT_MESSAGE               =  2_i16
      UNKNOWN_TOPIC_OR_PARTITION    =  3_i16
      INVALID_FETCH_SIZE            =  4_i16
      LEADER_NOT_AVAILABLE          =  5_i16
      NOT_LEADER_OR_FOLLOWER        =  6_i16
      REQUEST_TIMED_OUT             =  7_i16
      BROKER_NOT_AVAILABLE          =  8_i16
      REPLICA_NOT_AVAILABLE         =  9_i16
      MESSAGE_TOO_LARGE             = 10_i16
      STALE_CONTROLLER_EPOCH        = 11_i16
      OFFSET_METADATA_TOO_LARGE     = 12_i16
      NETWORK_EXCEPTION             = 13_i16
      COORDINATOR_LOAD_IN_PROGRESS  = 14_i16
      COORDINATOR_NOT_AVAILABLE     = 15_i16
      NOT_COORDINATOR               = 16_i16
      INVALID_TOPIC_EXCEPTION       = 17_i16
      RECORD_LIST_TOO_LARGE         = 18_i16
      NOT_ENOUGH_REPLICAS           = 19_i16
      NOT_ENOUGH_REPLICAS_AFTER_APPEND = 20_i16
      INVALID_REQUIRED_ACKS         = 21_i16
      ILLEGAL_GENERATION            = 22_i16
      INCONSISTENT_GROUP_PROTOCOL   = 23_i16
      INVALID_GROUP_ID              = 24_i16
      UNKNOWN_MEMBER_ID             = 25_i16
      INVALID_SESSION_TIMEOUT       = 26_i16
      REBALANCE_IN_PROGRESS         = 27_i16
      INVALID_COMMIT_OFFSET_SIZE    = 28_i16
      TOPIC_AUTHORIZATION_FAILED    = 29_i16
      GROUP_AUTHORIZATION_FAILED    = 30_i16
      CLUSTER_AUTHORIZATION_FAILED  = 31_i16
      INVALID_TIMESTAMP             = 32_i16
      UNSUPPORTED_SASL_MECHANISM    = 33_i16
      ILLEGAL_SASL_STATE            = 34_i16
      UNSUPPORTED_VERSION           = 35_i16
      TOPIC_ALREADY_EXISTS          = 36_i16
      INVALID_PARTITIONS            = 37_i16
      INVALID_REPLICATION_FACTOR    = 38_i16
      INVALID_REPLICA_ASSIGNMENT    = 39_i16
      INVALID_CONFIG                = 40_i16
      NOT_CONTROLLER                = 41_i16
      INVALID_REQUEST               = 42_i16
      UNSUPPORTED_FOR_MESSAGE_FORMAT = 43_i16
      POLICY_VIOLATION              = 44_i16
    end

    # Supported API versions
    SUPPORTED_API_VERSIONS = [
      {ApiKey::ApiVersions, 0_i16, 3_i16},
      {ApiKey::Metadata, 0_i16, 1_i16},
      {ApiKey::Produce, 0_i16, 3_i16},
    ]

    # Node ID for this broker (single node = 1)
    NODE_ID = 1_i32

    # Cluster ID
    CLUSTER_ID = "lavinmq-kafka-cluster"
  end
end
