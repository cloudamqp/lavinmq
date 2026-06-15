require "socket"

module LavinMQ
  module Clustering
    module VR
      # Control-plane wire protocol for the Viewstamped Replication coordinator.
      # These messages flow on a separate full-mesh of control connections
      # (see control_mesh.cr), kept off the data-replication stream so heartbeat
      # liveness isn't coupled to replication backpressure. A control connection
      # opens with the VRCTL header (distinct from the data stream's REPLI) and
      # then exchanges length-agnostic, fixed-layout little-endian messages, each
      # a one-byte Type tag followed by its fields.
      module Control
        # 8-byte start header for a control connection, mirroring Clustering::Start.
        HEADER = Bytes['V'.ord, 'R'.ord, 'C'.ord, 'T'.ord, 'L'.ord, 1, 0, 0]

        enum Type : UInt8
          Heartbeat       = 1
          StartViewChange = 2
          DoViewChange    = 3
          StartView       = 4
        end

        # Primary → backups, periodically. Carries the primary's view, its log
        # head (op) and commit point so backups can advance their own commit_op
        # out of band; receiving it also resets a backup's view-change timer.
        record Heartbeat, view : UInt64, op : UInt64, commit_op : UInt64, from_id : Int32 do
          def to_io(io : IO) : Nil
            io.write_bytes Type::Heartbeat.value, IO::ByteFormat::LittleEndian
            io.write_bytes view, IO::ByteFormat::LittleEndian
            io.write_bytes op, IO::ByteFormat::LittleEndian
            io.write_bytes commit_op, IO::ByteFormat::LittleEndian
            io.write_bytes from_id, IO::ByteFormat::LittleEndian
          end
        end

        # Sent when a backup's view-change timer fires (or it sees a higher view):
        # "I want to move to `view`". A node collects a quorum of these for a view
        # before sending DoViewChange to that view's prospective primary.
        record StartViewChange, view : UInt64, from_id : Int32 do
          def to_io(io : IO) : Nil
            io.write_bytes Type::StartViewChange.value, IO::ByteFormat::LittleEndian
            io.write_bytes view, IO::ByteFormat::LittleEndian
            io.write_bytes from_id, IO::ByteFormat::LittleEndian
          end
        end

        # Sent to the prospective primary of `view` once a quorum of
        # StartViewChange has been seen. Carries this node's log summary
        # (last_normal_view, op, commit_op) so the new primary can pick the most
        # up-to-date log among the quorum.
        record DoViewChange, view : UInt64, last_normal_view : UInt64, op : UInt64, commit_op : UInt64, from_id : Int32 do
          def to_io(io : IO) : Nil
            io.write_bytes Type::DoViewChange.value, IO::ByteFormat::LittleEndian
            io.write_bytes view, IO::ByteFormat::LittleEndian
            io.write_bytes last_normal_view, IO::ByteFormat::LittleEndian
            io.write_bytes op, IO::ByteFormat::LittleEndian
            io.write_bytes commit_op, IO::ByteFormat::LittleEndian
            io.write_bytes from_id, IO::ByteFormat::LittleEndian
          end
        end

        # The deterministic primary_of(view) → all backups, once it has collected
        # a quorum of DoViewChange: "view `view` is now normal and `primary_id`
        # (the most up-to-date node in the quorum) is its primary". Naming the
        # winner in a single message from a single decider keeps every node's
        # choice of primary consistent. Backups adopt the view and follow
        # `primary_id`; the named primary starts serving.
        record StartView, view : UInt64, primary_id : Int32, op : UInt64, commit_op : UInt64, from_id : Int32 do
          def to_io(io : IO) : Nil
            io.write_bytes Type::StartView.value, IO::ByteFormat::LittleEndian
            io.write_bytes view, IO::ByteFormat::LittleEndian
            io.write_bytes primary_id, IO::ByteFormat::LittleEndian
            io.write_bytes op, IO::ByteFormat::LittleEndian
            io.write_bytes commit_op, IO::ByteFormat::LittleEndian
            io.write_bytes from_id, IO::ByteFormat::LittleEndian
          end
        end

        alias Message = Heartbeat | StartViewChange | DoViewChange | StartView

        # Read one message, dispatching on the leading Type tag. Raises
        # IO::EOFError when the connection closes.
        def self.read(io : IO) : Message
          tag = io.read_byte || raise IO::EOFError.new
          case Type.new(tag)
          in Type::Heartbeat
            Heartbeat.new(
              view: io.read_bytes(UInt64, IO::ByteFormat::LittleEndian),
              op: io.read_bytes(UInt64, IO::ByteFormat::LittleEndian),
              commit_op: io.read_bytes(UInt64, IO::ByteFormat::LittleEndian),
              from_id: io.read_bytes(Int32, IO::ByteFormat::LittleEndian),
            )
          in Type::StartViewChange
            StartViewChange.new(
              view: io.read_bytes(UInt64, IO::ByteFormat::LittleEndian),
              from_id: io.read_bytes(Int32, IO::ByteFormat::LittleEndian),
            )
          in Type::DoViewChange
            DoViewChange.new(
              view: io.read_bytes(UInt64, IO::ByteFormat::LittleEndian),
              last_normal_view: io.read_bytes(UInt64, IO::ByteFormat::LittleEndian),
              op: io.read_bytes(UInt64, IO::ByteFormat::LittleEndian),
              commit_op: io.read_bytes(UInt64, IO::ByteFormat::LittleEndian),
              from_id: io.read_bytes(Int32, IO::ByteFormat::LittleEndian),
            )
          in Type::StartView
            StartView.new(
              view: io.read_bytes(UInt64, IO::ByteFormat::LittleEndian),
              primary_id: io.read_bytes(Int32, IO::ByteFormat::LittleEndian),
              op: io.read_bytes(UInt64, IO::ByteFormat::LittleEndian),
              commit_op: io.read_bytes(UInt64, IO::ByteFormat::LittleEndian),
              from_id: io.read_bytes(Int32, IO::ByteFormat::LittleEndian),
            )
          end
        end
      end
    end
  end
end
