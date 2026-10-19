#!/usr/bin/env bash
# Sets up one Raspberry Pi as a node of the booth's 3-node LavinMQ cluster.
# Run as root on each Pi, with the lavinmq and lavinmqctl arm64 binaries and
# the shared clustering password file next to this script:
#
#   sudo ./setup.sh <node number> <ip of pi-1> <ip of pi-2> <ip of pi-3> [--bootstrap]
#
# Pass --bootstrap on pi-1 only, and only the very first time: a new raft
# cluster needs one node allowed to lead before any node has election state.
set -euo pipefail

usage() { echo "usage: $0 <1|2|3> <ip1> <ip2> <ip3> [--bootstrap]" >&2; exit 2; }
[[ $# -ge 4 ]] || usage
N=$1; IPS=("$2" "$3" "$4"); BOOTSTRAP=${5:-}
[[ $N =~ ^[123]$ ]] || usage
IP=${IPS[$((N - 1))]}
NAME=pi-$N
HERE=$(cd "$(dirname "$0")" && pwd)

for f in lavinmq lavinmqctl clustering_password; do
  [[ -f "$HERE/$f" ]] || { echo "$HERE/$f missing, see README.md" >&2; exit 1; }
done
if [[ $(uname -m) != aarch64 ]]; then
  echo "This needs a 64-bit OS (uname -m is $(uname -m))" >&2
  exit 1
fi

install -m 0755 "$HERE/lavinmq" "$HERE/lavinmqctl" /usr/local/bin/
id lavinmq >/dev/null 2>&1 || useradd --system --home-dir /var/lib/lavinmq --shell /usr/sbin/nologin lavinmq
install -d -o lavinmq -g lavinmq -m 0750 /var/lib/lavinmq
install -d -m 0755 /etc/lavinmq
install -o lavinmq -g lavinmq -m 0600 "$HERE/clustering_password" /etc/lavinmq/clustering_password

bootstrap_line=""
if [[ $BOOTSTRAP == --bootstrap ]]; then
  [[ -e /var/lib/lavinmq/.raft_state ]] && { echo "Already has raft state, not bootstrapping" >&2; exit 1; }
  bootstrap_line="bootstrap = true"
fi
sed -e "s/{{NAME}}/$NAME/g" -e "s/{{IP}}/$IP/g" \
  -e "s/{{SEEDS}}/${IPS[0]},${IPS[1]},${IPS[2]}/" \
  -e "s/{{BOOTSTRAP}}/$bootstrap_line/" \
  "$HERE/lavinmq.ini.tmpl" >/etc/lavinmq/lavinmq.ini

# Message segments are mmap'd; the default limit is low for a broker
echo "vm.max_map_count = 1000000" >/etc/sysctl.d/90-lavinmq.conf
sysctl -q --system

install -m 0644 "$HERE/lavinmq.service" /etc/systemd/system/lavinmq.service
hostnamectl set-hostname "$NAME"
systemctl daemon-reload
systemctl enable --now lavinmq
echo "$NAME ($IP) started. Check with: journalctl -u lavinmq -f"
