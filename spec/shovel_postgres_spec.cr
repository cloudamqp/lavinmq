require "./spec_helper"
require "../src/lavinmq/shovel"

module PgOutputSpecHelpers
  # Builds pgoutput protocol messages the way the server encodes them.
  class Msg
    getter io = IO::Memory.new

    def initialize(type : Char)
      @io.write_byte type.ord.to_u8
    end

    def u8(v)
      @io.write_byte v.to_u8
      self
    end

    def i16(v)
      @io.write_bytes v.to_i16, IO::ByteFormat::BigEndian
      self
    end

    def i32(v)
      @io.write_bytes v.to_i32, IO::ByteFormat::BigEndian
      self
    end

    def i64(v)
      @io.write_bytes v.to_i64, IO::ByteFormat::BigEndian
      self
    end

    def str(v : String)
      @io << v
      @io.write_byte 0_u8
      self
    end

    # Tuple data: nil is a NULL, :unchanged an unchanged TOAST value
    def tuple(values : Array(String | Symbol?))
      i16(values.size)
      values.each do |v|
        case v
        when nil        then u8('n'.ord)
        when :unchanged then u8('u'.ord)
        when String
          u8('t'.ord)
          i32(v.bytesize)
          @io << v
        end
      end
      self
    end

    def bytes
      @io.to_slice
    end
  end

  def self.begin_tx(xid = 7)
    Msg.new('B').i64(0x100).i64(0).i32(xid).bytes
  end

  def self.commit(commit_lsn = 0x1_0000_0100_i64, end_lsn = 0x1_0000_0200_i64)
    Msg.new('C').u8(0).i64(commit_lsn).i64(end_lsn).i64(1_000_000).bytes
  end

  # relation 1: public.users(id int4 key, name text, active bool, data jsonb, score float8)
  def self.relation(id = 1)
    m = Msg.new('R').i32(id).str("public").str("users").u8('d'.ord).i16(5)
    m.u8(1).str("id").i32(23).i32(-1)
    m.u8(0).str("name").i32(25).i32(-1)
    m.u8(0).str("active").i32(16).i32(-1)
    m.u8(0).str("data").i32(3802).i32(-1)
    m.u8(0).str("score").i32(701).i32(-1)
    m.bytes
  end

  def self.decode_tx(*messages : Bytes) : LavinMQ::Shovel::Postgres::PgOutput::Transaction
    decoder = LavinMQ::Shovel::Postgres::PgOutput.new
    tx = nil
    messages.each { |m| tx = decoder.decode(m) || tx }
    tx || fail "No transaction decoded"
  end

  # Specs against a real server only run when a PostgreSQL with
  # wal_level=logical is given, e.g. postgres://postgres:secret@localhost/postgres
  def self.pg_uri : URI?
    ENV["LAVINMQ_SPEC_PG_URI"]?.try { |u| URI.parse(u) }
  end

  def self.with_sql(uri, &)
    conn = LavinMQ::Shovel::Postgres::Connection.connect(uri, replication: false)
    yield conn
  ensure
    conn.try &.close
  end

  def self.setup_table(uri, table, slot)
    with_sql(uri) do |sql|
      sql.query("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = '#{slot}'")
      sql.query("DROP PUBLICATION IF EXISTS #{table}_pub")
      sql.query("DROP TABLE IF EXISTS #{table}")
      sql.query("CREATE TABLE #{table} (id int PRIMARY KEY, name text)")
      sql.query("CREATE PUBLICATION #{table}_pub FOR TABLE #{table}")
    end
  end

  # What a server sends after the startup message: AuthenticationOk, the
  # ParameterStatus messages and ReadyForQuery.
  def self.startup_reply(parameters : Hash(String, String)) : IO::Memory
    server = IO::Memory.new
    server << 'R'
    server.write_bytes 8_i32, IO::ByteFormat::BigEndian
    server.write_bytes 0_i32, IO::ByteFormat::BigEndian
    parameters.each do |k, v|
      param = "#{k}\0#{v}\0"
      server << 'S'
      server.write_bytes (param.bytesize + 4).to_i32, IO::ByteFormat::BigEndian
      server << param
    end
    server << 'Z'
    server.write_bytes 5_i32, IO::ByteFormat::BigEndian
    server << 'I'
    server.rewind
    server
  end

  def self.slot_lsn(uri, slot) : UInt64
    with_sql(uri) do |sql|
      rows = sql.query("SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '#{slot}'")
      LavinMQ::Shovel::Postgres::LSN.parse(rows.dig?(0, 0) || "0/0")
    end
  end

  def self.drop_slot(uri, slot)
    with_sql(uri) do |sql|
      sql.query("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = '#{slot}'")
    end
  end

  # Runs the source until `count` transactions are yielded, then stops it.
  def self.take(source, count) : Array(LavinMQ::Endpoint::Delivery)
    msgs = [] of LavinMQ::Endpoint::Delivery
    source.start
    done = Channel(Nil).new
    spawn do
      source.each do |m|
        msgs << m
        done.send nil if msgs.size == count
      end
    rescue
    end
    select
    when done.receive
    when timeout(10.seconds)
      fail "Expected #{count} transactions, got #{msgs.size}"
    end
    msgs
  end
end

# Reaches into PostgresSource to feed it decoded transactions without a server
class LavinMQ::Shovel::PostgresSource
  def spec_register(tx)
    register(tx)
  end

  def spec_pending
    @pending.to_a
  end
end

describe "PostgreSQL shovel" do
  describe "PgOutput" do
    it "collects a transaction's changes into one JSON message" do
      h = PgOutputSpecHelpers
      insert = PgOutputSpecHelpers::Msg.new('I').i32(1).u8('N'.ord)
        .tuple(["1", "alice", "t", %({"a": [1, 2]}), "1.5"]).bytes
      update = PgOutputSpecHelpers::Msg.new('U').i32(1).u8('K'.ord)
        .tuple(["1", nil, nil, nil, nil]).u8('N'.ord)
        .tuple(["2", "bob", "f", :unchanged, "NaN"]).bytes
      delete = PgOutputSpecHelpers::Msg.new('D').i32(1).u8('O'.ord)
        .tuple(["2", "bob", "f", nil, "-Infinity"]).bytes
      tx = h.decode_tx(h.begin_tx(42), h.relation, insert, update, delete, h.commit)

      tx.xid.should eq 42
      tx.change_count.should eq 3
      tx.end_lsn.should eq 0x1_0000_0200
      body = JSON.parse(String.new(tx.body))
      body["xid"].should eq 42
      body["lsn"].should eq "1/100"
      body["commit_time"].should eq "2000-01-01T00:00:01.000000Z"
      changes = body["changes"].as_a
      changes[0].should eq JSON.parse({op: "insert", schema: "public", table: "users",
                                       new: {id: 1, name: "alice", active: true, data: {a: [1, 2]}, score: 1.5}}.to_json)
      # 'K' tuple: only the key columns; unchanged TOAST values are left out
      changes[1].should eq JSON.parse({op: "update", schema: "public", table: "users",
                                       key: {id: 1}, new: {id: 2, name: "bob", active: false, score: "NaN"}}.to_json)
      # 'O' tuple (REPLICA IDENTITY FULL): the whole old row, NULLs included
      changes[2].should eq JSON.parse({op: "delete", schema: "public", table: "users",
                                       old: {id: 2, name: "bob", active: false, data: nil, score: "-Infinity"}}.to_json)
    end

    it "decodes truncate and update without old tuple" do
      h = PgOutputSpecHelpers
      update = PgOutputSpecHelpers::Msg.new('U').i32(1).u8('N'.ord).tuple(["1", "x", nil, nil, nil]).bytes
      truncate = PgOutputSpecHelpers::Msg.new('T').i32(1).u8(0).i32(1).bytes
      tx = h.decode_tx(h.begin_tx, h.relation, update, truncate, h.commit)
      changes = JSON.parse(String.new(tx.body))["changes"].as_a
      changes[0].as_h.has_key?("key").should be_false
      changes[0]["new"].should eq JSON.parse(%({"id":1,"name":"x","active":null,"data":null,"score":null}))
      changes[1].should eq JSON.parse(%({"op":"truncate","schema":"public","table":"users"}))
    end

    it "starts each transaction empty" do
      h = PgOutputSpecHelpers
      decoder = LavinMQ::Shovel::Postgres::PgOutput.new
      decoder.decode(h.relation)
      [h.begin_tx(1), PgOutputSpecHelpers::Msg.new('I').i32(1).u8('N'.ord).tuple(["1", nil, nil, nil, nil]).bytes].each do |m|
        decoder.decode(m).should be_nil
      end
      decoder.in_transaction?.should be_true
      decoder.decode(h.commit).try(&.change_count).should eq 1
      decoder.in_transaction?.should be_false
      decoder.decode(h.begin_tx(2))
      tx = decoder.decode(h.commit) || fail "No transaction decoded"
      tx.empty?.should be_true
      JSON.parse(String.new(tx.body))["changes"].as_a.should be_empty
    end

    it "uses the latest Relation message for a relation" do
      h = PgOutputSpecHelpers
      renamed = PgOutputSpecHelpers::Msg.new('R').i32(1).str("app").str("people").u8('d'.ord).i16(1)
        .u8(1).str("pk").i32(20).i32(-1).bytes
      insert = PgOutputSpecHelpers::Msg.new('I').i32(1).u8('N'.ord).tuple(["9007199254740993"]).bytes
      tx = h.decode_tx(h.relation, renamed, h.begin_tx, insert, h.commit)
      String.new(tx.body).should contain %("schema":"app","table":"people","new":{"pk":9007199254740993})
    end

    it "raises on an unknown relation" do
      h = PgOutputSpecHelpers
      insert = PgOutputSpecHelpers::Msg.new('I').i32(5).u8('N'.ord).tuple(["1"]).bytes
      decoder = LavinMQ::Shovel::Postgres::PgOutput.new
      decoder.decode(h.begin_tx)
      expect_raises(LavinMQ::Shovel::Postgres::Error, /Unknown relation/) do
        decoder.decode(insert)
      end
    end
  end

  describe "LSN" do
    it "formats and parses" do
      LavinMQ::Shovel::Postgres::LSN.format(0x16_B374D848_u64).should eq "16/B374D848"
      LavinMQ::Shovel::Postgres::LSN.parse("16/B374D848").should eq 0x16_B374D848_u64
    end
  end

  describe "SCRAM" do
    it "computes the RFC 7677 client proof and verifies the server" do
      scram = LavinMQ::Shovel::Postgres::SCRAM.new("pencil", "rOprNGfwEbeRWgbNEkqO", "user")
      scram.client_first.should eq "n,,n=user,r=rOprNGfwEbeRWgbNEkqO"
      server_first = "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096"
      scram.client_final(server_first).should eq \
        "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="
      scram.verify!("v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=")
      expect_raises(LavinMQ::Shovel::Postgres::Error, /mismatch/) do
        scram.verify!("v=AAAATRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=")
      end
    end

    it "rejects a server nonce that doesn't extend the client nonce" do
      scram = LavinMQ::Shovel::Postgres::SCRAM.new("pencil", "abc")
      expect_raises(LavinMQ::Shovel::Postgres::Error, /nonce/) do
        scram.client_final("r=xyz,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096")
      end
    end
  end

  describe "Store.validate_config!" do
    base = {"src-uri" => "postgres://u:p@localhost/db", "dest-uri" => "amqp://", "dest-exchange" => "amq.topic"}

    it "accepts a publication instead of a queue or exchange" do
      LavinMQ::Shovel::Store.validate_config!(JSON.parse(base.merge({"src-publication" => "pub"}).to_json), nil)
    end

    it "requires src-publication" do
      expect_raises(LavinMQ::Shovel::ConfigError, /src-publication/) do
        LavinMQ::Shovel::Store.validate_config!(JSON.parse(base.to_json), nil)
      end
    end

    it "rejects an invalid slot name" do
      config = base.merge({"src-publication" => "pub", "src-slot" => "Bad-Slot"})
      expect_raises(LavinMQ::Shovel::ConfigError, /src-slot/) do
        LavinMQ::Shovel::Store.validate_config!(JSON.parse(config.to_json), nil)
      end
    end

    it "rejects an unknown sslmode" do
      config = base.merge({"src-publication" => "pub", "src-uri" => "postgres://u:p@localhost/db?sslmode=verify_full"})
      expect_raises(LavinMQ::Shovel::ConfigError, /sslmode/) do
        LavinMQ::Shovel::Store.validate_config!(JSON.parse(config.to_json), nil)
      end
    end

    it "rejects src-delete-after queue-length" do
      config = base.merge({"src-publication" => "pub", "src-delete-after" => "queue-length"})
      expect_raises(LavinMQ::Shovel::ConfigError, /queue-length/) do
        LavinMQ::Shovel::Store.validate_config!(JSON.parse(config.to_json), nil)
      end
    end
  end

  describe "PostgresSource#register" do
    tx = ->(end_lsn : UInt64, changes : Int32) do
      LavinMQ::Shovel::Postgres::PgOutput::Transaction.new(1_u32, end_lsn - 1, end_lsn, Time.utc, changes, Bytes.empty)
    end

    it "flushes a transaction without published changes when nothing is in flight" do
      source = LavinMQ::Shovel::PostgresSource.new("spec", URI.parse("postgres://u@h/db"), "pub", "slot")
      source.spec_register(tx.call(100_u64, 0)).should be_nil
      source.spec_pending.should be_empty
      source.flushed_lsn.should eq 100
    end

    it "keeps transactions without published changes out of the prefetch window" do
      source = LavinMQ::Shovel::PostgresSource.new("spec", URI.parse("postgres://u@h/db"), "pub", "slot", prefetch: 2_u16)
      tag = source.spec_register(tx.call(100_u64, 1)) || fail "No delivery tag"
      5.times { |i| source.spec_register(tx.call(200_u64 + i, 0)).should be_nil }
      # Flushed once the published transaction before them settles
      source.spec_pending.should eq [{tag, 204_u64}]
      source.flushed_lsn.should eq 0
    end
  end

  describe "Connection" do
    it "accepts the known sslmodes and defaults to prefer" do
      conn = LavinMQ::Shovel::Postgres::Connection
      conn.sslmode(URI.parse("postgres://u@h/db")).should eq "prefer"
      %w[disable prefer require verify-full].each do |mode|
        conn.sslmode(URI.parse("postgres://u@h/db?sslmode=#{mode}")).should eq mode
      end
      conn.sslmode(URI.parse("postgres://u@h/db?sslmode=verify-ca&sslrootcert=/ca.pem")).should eq "verify-ca"
    end

    it "requires sslrootcert for verify-ca, which checks no host name" do
      expect_raises(LavinMQ::Shovel::Postgres::Error, /sslrootcert/) do
        LavinMQ::Shovel::Postgres::Connection.sslmode(URI.parse("postgres://u@h/db?sslmode=verify-ca"))
      end
    end

    it "trusts only sslrootcert, not the system CAs as well" do
      resources = "#{__DIR__}/resources"
      # The server certificate's CA stands in for one in the system store
      system_cas = ENV["SSL_CERT_FILE"]?
      ENV["SSL_CERT_FILE"] = "#{resources}/ca_certificate.pem"
      server = TCPServer.new("127.0.0.1", 0)
      spawn do
        client = server.accept
        client.read_fully(Bytes.new(8)) # SSLRequest
        client << 'S'
        client.flush
        ctx = OpenSSL::SSL::Context::Server.new
        ctx.certificate_chain = "#{resources}/server_certificate.pem"
        ctx.private_key = "#{resources}/server_key.pem"
        OpenSSL::SSL::Socket::Server.new(client, ctx, sync_close: true).close
      rescue
        client.try &.close
      end
      # A CA unrelated to the server's certificate
      uri = URI.parse("postgres://u@127.0.0.1:#{server.local_address.port}/db?sslmode=verify-ca" \
                      "&sslrootcert=#{resources}/foobar_localhost_certificate.pem")
      expect_raises(LavinMQ::Shovel::Postgres::Error, /certificate verify failed/) do
        LavinMQ::Shovel::Postgres::Connection.connect(uri)
      end
    ensure
      if system_cas
        ENV["SSL_CERT_FILE"] = system_cas
      else
        ENV.delete("SSL_CERT_FILE")
      end
      server.try &.close
    end

    it "refuses to connect with an unknown sslmode instead of skipping verification" do
      expect_raises(LavinMQ::Shovel::Postgres::Error, /Unsupported sslmode 'verify_full'/) do
        # Checked before connecting: nothing listens on port 1
        LavinMQ::Shovel::Postgres::Connection.connect(URI.parse("postgres://u@127.0.0.1:1/db?sslmode=verify_full"))
      end
    end

    it "refuses a replication connection to PostgreSQL 13 or older" do
      {"13.12" => false, "14.0" => true, "16.4 (Ubuntu 16.4-1)" => true, "17beta1" => true}.each do |version, ok|
        server = PgOutputSpecHelpers.startup_reply({"server_version" => version})
        conn = LavinMQ::Shovel::Postgres::Connection.new(IO::Stapled.new(server, IO::Memory.new))
        if ok
          conn.startup(URI.parse("postgres://u@h/db"), "spec", true)
        else
          expect_raises(LavinMQ::Shovel::Postgres::Error, /PostgreSQL 14 or later is required, the server is 13.12/) do
            conn.startup(URI.parse("postgres://u@h/db"), "spec", true)
          end
        end
      end
    end

    it "accepts a server parameter with an empty value" do
      server = PgOutputSpecHelpers.startup_reply({"default_tablespace" => ""})
      conn = LavinMQ::Shovel::Postgres::Connection.new(IO::Stapled.new(server, IO::Memory.new))
      conn.startup(URI.parse("postgres://u@h/db"), "spec", false)
      conn.parameters["default_tablespace"].should eq ""
    end

    it "closes the socket when TLS can't be negotiated" do
      {"N" => /does not support TLS/, "S" => /Could not connect/}.each do |reply, error|
        server = TCPServer.new("127.0.0.1", 0)
        closed = Channel(Bool).new(1)
        spawn do
          client = server.accept
          client.read_fully(Bytes.new(8)) # SSLRequest
          client << reply
          client << "not a TLS handshake" * 10 if reply == "S"
          client.flush
          buf = Bytes.new(1024)
          # The client closing its end shows up as EOF
          while client.read(buf) > 0
          end
          closed.send true
        rescue IO::Error
          closed.send true
        end
        uri = URI.parse("postgres://u@127.0.0.1:#{server.local_address.port}/db?sslmode=require")
        expect_raises(LavinMQ::Shovel::Postgres::Error, error) do
          LavinMQ::Shovel::Postgres::Connection.connect(uri)
        end
        select
        when closed.receive
        when timeout(5.seconds)
          fail "Client socket left open after reply #{reply}"
        end
      ensure
        server.try &.close
      end
    end

    it "percent-decodes the database name" do
      conn = LavinMQ::Shovel::Postgres::Connection
      conn.database(URI.parse("postgres://u@h/db%20name"), "u").should eq "db name"
      conn.database(URI.parse("postgres://u@h"), "u").should eq "u"
    end

    it "connects to an IPv6 address" do
      server = begin
        TCPServer.new("::1", 0)
      rescue Socket::Error
        next pending!("No IPv6 loopback")
      end
      spawn do
        if client = server.accept?
          len = client.read_bytes(Int32, IO::ByteFormat::BigEndian) # startup message
          client.skip(len - 4)
          client.close
        end
      end
      uri = URI.parse("postgres://u@[::1]:#{server.local_address.port}/db?sslmode=disable")
      # Reached the server, rather than failing to look up "[::1]"
      expect_raises(LavinMQ::Shovel::Postgres::Error, /closed by server/) do
        LavinMQ::Shovel::Postgres::Connection.connect(uri)
      end
    ensure
      server.try &.close
    end
  end

  it "derives a valid default slot name from the shovel name" do
    LavinMQ::Shovel::PostgresSource.default_slot_name("My Shovel-1").should eq "lavinmq_my_shovel_1"
    LavinMQ::Shovel::PostgresSource.default_slot_name("x" * 100).size.should eq 63
  end

  describe "PostgresSource" do
    it "streams committed transactions and resumes after the last acked one" do
      uri = PgOutputSpecHelpers.pg_uri || next pending!("LAVINMQ_SPEC_PG_URI not set")
      slot = "lavinmq_spec_resume"
      PgOutputSpecHelpers.setup_table(uri, "spec_resume", slot)
      source = LavinMQ::Shovel::PostgresSource.new("spec", uri, "spec_resume_pub", slot)
      begin
        source.start
        source.stop # creates the slot, changes are captured from here on
        PgOutputSpecHelpers.with_sql(uri) do |sql|
          sql.query("BEGIN; INSERT INTO spec_resume VALUES (1, 'a'); INSERT INTO spec_resume VALUES (2, 'b'); COMMIT")
          sql.query("UPDATE spec_resume SET name = 'c' WHERE id = 1")
          sql.query("DELETE FROM spec_resume WHERE id = 2")
        end

        msgs = PgOutputSpecHelpers.take(source, 3)
        first = JSON.parse(String.new(msgs[0].body))
        first["changes"].as_a.map(&.["op"]).should eq ["insert", "insert"]
        first["changes"][1]["new"].should eq JSON.parse(%({"id":2,"name":"b"}))
        msgs[0].routing_key.should eq "spec_resume_pub"
        msgs[0].properties.content_type.should eq "application/json"
        msgs[0].properties.message_id.should eq first["lsn"].as_s
        JSON.parse(String.new(msgs[1].body))["changes"][0]["op"].should eq "update"
        JSON.parse(String.new(msgs[2].body))["changes"][0]["key"].should eq JSON.parse(%({"id":2}))

        # Acked out of order: the frontier can't pass the unacked first one
        source.ack(msgs[1].tag)
        source.ack(msgs[2].tag)
        source.stop
        again = PgOutputSpecHelpers.take(source, 3)
        again.map(&.properties.message_id).should eq msgs.map(&.properties.message_id)

        # Acked up to the second: only the third comes back
        source.ack(again[0].tag)
        source.ack(again[1].tag)
        source.stop
        last = PgOutputSpecHelpers.take(source, 1)
        last[0].properties.message_id.should eq msgs[2].properties.message_id
      ensure
        source.stop
        PgOutputSpecHelpers.drop_slot(uri, slot)
      end
    end

    it "streams again from the last acked transaction on a requeue" do
      uri = PgOutputSpecHelpers.pg_uri || next pending!("LAVINMQ_SPEC_PG_URI not set")
      slot = "lavinmq_spec_requeue"
      PgOutputSpecHelpers.setup_table(uri, "spec_requeue", slot)
      source = LavinMQ::Shovel::PostgresSource.new("spec", uri, "spec_requeue_pub", slot)
      begin
        source.start
        source.stop
        PgOutputSpecHelpers.with_sql(uri) do |sql|
          sql.query("INSERT INTO spec_requeue VALUES (1, 'a')")
          sql.query("INSERT INTO spec_requeue VALUES (2, 'b')")
        end
        source.start
        msgs = [] of LavinMQ::Endpoint::Delivery
        error = Channel(Exception).new(1)
        spawn do
          source.each do |m|
            msgs << m
            if msgs.size == 1
              source.ack(m.tag)
            else
              source.reject(m.tag, requeue: true)
            end
          end
        rescue ex
          error.send ex
        end
        select
        when ex = error.receive
          ex.message.to_s.should contain "streaming again"
        when timeout(10.seconds)
          fail "each didn't raise on requeue"
        end
        source.started?.should be_false
        again = PgOutputSpecHelpers.take(source, 1)
        again[0].properties.message_id.should eq msgs[1].properties.message_id
      ensure
        source.stop
        PgOutputSpecHelpers.drop_slot(uri, slot)
      end
    end

    # PostgreSQL 15+ doesn't stream transactions without published changes,
    # so this only exercises the empty transaction path against 14.
    it "doesn't let transactions outside the publication fill the prefetch window" do
      uri = PgOutputSpecHelpers.pg_uri || next pending!("LAVINMQ_SPEC_PG_URI not set")
      slot = "lavinmq_spec_unpublished"
      PgOutputSpecHelpers.setup_table(uri, "spec_published", slot)
      PgOutputSpecHelpers.with_sql(uri) do |sql|
        sql.query("DROP TABLE IF EXISTS spec_unpublished")
        sql.query("CREATE TABLE spec_unpublished (id int)")
      end
      source = LavinMQ::Shovel::PostgresSource.new("spec", uri, "spec_published_pub", slot, prefetch: 2_u16)
      begin
        source.start
        source.stop
        PgOutputSpecHelpers.with_sql(uri) do |sql|
          sql.query("INSERT INTO spec_published VALUES (1, 'a')")
          3.times { |i| sql.query("INSERT INTO spec_unpublished VALUES (#{i})") }
          sql.query("INSERT INTO spec_published VALUES (2, 'b')")
        end
        # Neither is acked: the first holds one of the two slots
        msgs = PgOutputSpecHelpers.take(source, 2)
        msgs.each { |m| source.ack(m.tag) }
        last_commit = LavinMQ::Shovel::Postgres::LSN.parse(msgs[1].properties.message_id || "0/0")
        should_eventually(be_true) do
          PgOutputSpecHelpers.slot_lsn(uri, slot) > last_commit
        end
      ensure
        source.stop
        PgOutputSpecHelpers.drop_slot(uri, slot)
      end
    end

    it "reports the slot position when the first delivery is requeued" do
      uri = PgOutputSpecHelpers.pg_uri || next pending!("LAVINMQ_SPEC_PG_URI not set")
      slot = "lavinmq_spec_first_requeue"
      PgOutputSpecHelpers.setup_table(uri, "spec_first_requeue", slot)
      source = LavinMQ::Shovel::PostgresSource.new("spec", uri, "spec_first_requeue_pub", slot)
      begin
        source.start
        source.stop
        PgOutputSpecHelpers.with_sql(uri) { |sql| sql.query("INSERT INTO spec_first_requeue VALUES (1, 'a')") }
        slot_lsn = LavinMQ::Shovel::Postgres::LSN.format(PgOutputSpecHelpers.slot_lsn(uri, slot))
        source.start
        LavinMQ::Shovel::Postgres::LSN.format(source.flushed_lsn).should eq slot_lsn
        error = Channel(Exception).new(1)
        spawn do
          source.each { |m| source.reject(m.tag, requeue: true) }
        rescue ex
          error.send ex
        end
        select
        when ex = error.receive
          ex.message.should eq "Delivery failed, streaming again from #{slot_lsn}"
          slot_lsn.should_not eq "0/0"
        when timeout(10.seconds)
          fail "each didn't raise on requeue"
        end
      ensure
        source.stop
        PgOutputSpecHelpers.drop_slot(uri, slot)
      end
    end

    it "gives up a start that a stop overtook" do
      uri = PgOutputSpecHelpers.pg_uri || next pending!("LAVINMQ_SPEC_PG_URI not set")
      slot = "lavinmq_spec_stop_starting"
      PgOutputSpecHelpers.setup_table(uri, "spec_stop_starting", slot)
      source = LavinMQ::Shovel::PostgresSource.new("spec_stop_starting", uri, "spec_stop_starting_pub", slot)
      begin
        PgOutputSpecHelpers.with_sql(uri) do |blocker|
          # CREATE_REPLICATION_SLOT waits for this transaction to end
          blocker.query("BEGIN; INSERT INTO spec_stop_starting VALUES (1, 'a')")
          started = Channel(Exception?).new(1)
          spawn do
            source.start
            started.send nil
          rescue ex
            started.send ex
          end
          should_eventually(eq 1) do
            PgOutputSpecHelpers.with_sql(uri) do |sql|
              sql.query("SELECT 1 FROM pg_stat_activity WHERE wait_event_type = 'Lock' " \
                        "AND application_name = 'LavinMQ shovel spec_stop_starting'").size
            end
          end
          source.stop # paused or deleted while starting
          select
          when ex = started.receive
            ex.should_not be_nil
          when timeout(5.seconds)
            fail "start still blocked after stop"
          end
          blocker.query("COMMIT")
          source.started?.should be_false
        end
      ensure
        source.stop
        # The slot is created once the transaction ends, and released when its
        # walsender notices the closed connection and exits
        should_eventually(be_true) do
          PgOutputSpecHelpers.with_sql(uri) do |sql|
            sql.query("SELECT 1 FROM pg_stat_activity " \
                      "WHERE application_name = 'LavinMQ shovel spec_stop_starting'").empty?
          end
        end
        PgOutputSpecHelpers.drop_slot(uri, slot)
      end
    end

    it "delivers with an unlimited (0) prefetch" do
      uri = PgOutputSpecHelpers.pg_uri || next pending!("LAVINMQ_SPEC_PG_URI not set")
      slot = "lavinmq_spec_prefetch"
      PgOutputSpecHelpers.setup_table(uri, "spec_prefetch", slot)
      source = LavinMQ::Shovel::PostgresSource.new("spec", uri, "spec_prefetch_pub", slot, prefetch: 0_u16)
      begin
        source.start
        source.stop
        PgOutputSpecHelpers.with_sql(uri) do |sql|
          sql.query("INSERT INTO spec_prefetch VALUES (1, 'a')")
          sql.query("INSERT INTO spec_prefetch VALUES (2, 'b')")
        end
        PgOutputSpecHelpers.take(source, 2).size.should eq 2
      ensure
        source.stop
        PgOutputSpecHelpers.drop_slot(uri, slot)
      end
    end

    it "streams UTF-8 from a non UTF-8 database with an escaped name" do
      uri = PgOutputSpecHelpers.pg_uri || next pending!("LAVINMQ_SPEC_PG_URI not set")
      slot = "lavinmq_spec_latin1"
      db_uri = uri.dup
      db_uri.path = "/lavinmq%20spec%20latin1"
      PgOutputSpecHelpers.with_sql(uri) do |sql|
        sql.query(%(DROP DATABASE IF EXISTS "lavinmq spec latin1" WITH (FORCE)))
        sql.query(%(CREATE DATABASE "lavinmq spec latin1" ENCODING 'LATIN1' LC_COLLATE 'C' LC_CTYPE 'C' TEMPLATE template0))
      end
      PgOutputSpecHelpers.setup_table(db_uri, "spec_latin1", slot)
      source = LavinMQ::Shovel::PostgresSource.new("spec", db_uri, "spec_latin1_pub", slot)
      begin
        source.start
        source.stop
        PgOutputSpecHelpers.with_sql(db_uri) do |sql|
          # chr(233) is é whatever the client encoding of this connection
          sql.query("INSERT INTO spec_latin1 VALUES (1, 'caf' || chr(233))")
        end
        body = String.new(PgOutputSpecHelpers.take(source, 1)[0].body)
        body.valid_encoding?.should be_true
        JSON.parse(body)["changes"][0]["new"]["name"].should eq "café"
      ensure
        source.stop
        PgOutputSpecHelpers.drop_slot(db_uri, slot)
        PgOutputSpecHelpers.with_sql(uri) do |sql|
          sql.query(%(DROP DATABASE IF EXISTS "lavinmq spec latin1" WITH (FORCE)))
        end
      end
    end

    it "publishes table changes to an exchange" do
      uri = PgOutputSpecHelpers.pg_uri || next pending!("LAVINMQ_SPEC_PG_URI not set")
      slot = "lavinmq_spec_shovel"
      PgOutputSpecHelpers.setup_table(uri, "spec_shovel", slot)
      begin
        with_amqp_server do |s|
          with_channel(s) do |ch|
            q = ch.queue("pg_changes")
            q.bind("amq.topic", "spec_shovel_pub")
            config = {
              "src-uri":         uri.to_s,
              "src-publication": "spec_shovel_pub",
              "src-slot":        slot,
              "dest-uri":        "amqp://",
              "dest-exchange":   "amq.topic",
            }.to_json
            s.vhosts["/"].add_parameter(LavinMQ::Parameter.new("shovel", "pg", JSON.parse(config)))
            shovel = s.vhosts["/"].shovels["pg"]
            should_eventually(be_true) { shovel.running? }
            PgOutputSpecHelpers.with_sql(uri) do |sql|
              sql.query("BEGIN; INSERT INTO spec_shovel VALUES (1, 'a'); UPDATE spec_shovel SET name = 'b'; COMMIT")
            end
            should_eventually(eq 1) { q.message_count }
            msg = q.get(no_ack: true) || fail "No message"
            body = JSON.parse(msg.body_io.to_s)
            body["changes"].as_a.map(&.["op"]).should eq ["insert", "update"]
            msg.properties.headers.try(&.["x-pg-lsn"]).should eq body["lsn"].as_s
            # The slot is confirmed past the published transaction
            should_eventually(be_true) do
              PgOutputSpecHelpers.with_sql(uri) do |sql|
                flushed = sql.query("SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '#{slot}'")
                flushed_lsn = LavinMQ::Shovel::Postgres::LSN.parse(flushed.dig(0, 0) || "0/0")
                flushed_lsn > LavinMQ::Shovel::Postgres::LSN.parse(body["lsn"].as_s)
              end
            end
            s.vhosts["/"].delete_parameter("shovel", "pg")
          end
        end
      ensure
        PgOutputSpecHelpers.drop_slot(uri, slot)
      end
    end
  end
end
