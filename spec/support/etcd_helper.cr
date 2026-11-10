# Fail fast with a clear message when the etcd binary isn't installed,
# instead of each etcd spec burning its connect-retry loop before failing.
def ensure_etcd_in_path!
  return if Process.find_executable("etcd")
  raise "etcd binary not found in PATH, required by etcd-tagged specs. " \
        "Install etcd or exclude these specs with --tag=~etcd"
end

# Faster Raft timing for single member clusters; with more members, a stalled member would trigger elections
ETCD_SPEC_TIMING_FLAGS = {"--heartbeat-interval=25", "--election-timeout=250"}

# Client endpoint of the etcd that the innermost `with_etcd` started on free ports
def spec_etcd_endpoints : String
  SpecEtcd.endpoints.not_nil!("No etcd running, wrap the spec in with_etcd")
end

module SpecEtcd
  class_property endpoints : String?
end

def with_etcd(&)
  ensure_etcd_in_path!
  client_port, peer_port = free_port, free_port
  data_dir = File.tempname("etcd", "spec")
  p = Process.new("etcd", {
    "--data-dir=#{data_dir}",
    "--logger=zap",
    "--log-level=error",
    "--unsafe-no-fsync=true",
    "--force-new-cluster=true",
    *ETCD_SPEC_TIMING_FLAGS,
    "--listen-peer-urls=http://127.0.0.1:#{peer_port}",
    "--initial-advertise-peer-urls=http://127.0.0.1:#{peer_port}",
    "--initial-cluster=default=http://127.0.0.1:#{peer_port}",
    "--listen-client-urls=http://127.0.0.1:#{client_port}",
    "--advertise-client-urls=http://127.0.0.1:#{client_port}",
  }, output: STDOUT, error: STDERR)

  begin
    client = HTTP::Client.new("127.0.0.1", client_port)
    i = 0
    loop do
      sleep 0.02.seconds
      response = client.get("/version")
      if response.status.ok?
        next if response.body.includes? "not_decided"
        break
      end
    rescue e : IO::Error
      client.close
      i += 1
      raise "Can't connect to etcd on port #{client_port}. Giving up after 100 tries. (#{e.message})" if i >= 100
      next
    end
    client.close
    SpecEtcd.endpoints = "127.0.0.1:#{client_port}"
    yield
  ensure
    SpecEtcd.endpoints = nil
    # Reap etcd before removing its data, so it can't hold the ports and data dir the next example's etcd needs
    p.terminate(graceful: false) rescue nil
    p.wait rescue nil
    FileUtils.rm_rf data_dir
  end
end

macro add_etcd_around_each
  around_each do |spec|
    with_etcd { spec.run }
  end
end
