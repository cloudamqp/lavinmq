require "openssl"
require "socket"
require "./channel"
require "../vhost"
require "../user"
require "../stats"
require "../sortable_json"
require "../utils"
require "../reporter"

module LavinMQ
  alias ConnectionDetails = NamedTuple(
    peer_host: String,
    peer_port: Int32,
    name: String)

  abstract class Client
    include SortableJSON
    include Stats
    include Reportable

    rate_stats({"send_oct", "recv_oct"})
    reportables channels

    # alias ClientDetails = NamedTuple(...)
    Utils.alias_merged_tuple(
      ClientDetails,
      NamedTuple(
        channels: Int32,
        connected_at: Int64,
        type: String,
        channel_max: UInt16,
        frame_max: UInt32,
        timeout: UInt16,
        client_properties: AMQP::Table,
        vhost: String,
        user: String,
        protocol: String,
        auth_mechanism: String,
        host: String,
        port: Int32,
        peer_host: String,
        peer_port: Int32,
        name: String,
        pid: String,
        ssl: Bool,
        tls_version: String?,
        cipher: String?,
        state: String,
      ),
      # Merge in StatsDetails
      Client::StatsDetails
    )

    abstract def vhost : VHost
    abstract def channels : Hash(UInt16, Client::Channel)
    abstract def log : Log
    abstract def name : String
    abstract def user : User
    abstract def max_frame_size : UInt32
    abstract def channel_max : UInt16
    abstract def heartbeat_timeout : UInt16
    abstract def auth_mechanism : String
    abstract def client_properties : AMQP::Table
    abstract def remote_address : Socket::IPAddress
    abstract def client_name : String
    abstract def channel_name_prefix : String
    abstract def connection_details : ConnectionDetails
    abstract def state : String
    abstract def close(reason : String? = nil)
    abstract def force_close
    abstract def closed? : Bool
    abstract def details_tuple : ClientDetails
  end
end
