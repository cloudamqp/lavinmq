# Setting up the Pis

Three Raspberry Pis (4 or 5) run a 3-node LavinMQ cluster with the built-in
Raft leader election from [cloudamqp/lavinmq#2299](https://github.com/cloudamqp/lavinmq/pull/2299).
No etcd is needed. With the default 1.5 s election timeout, a new leader
serves about 2–3 s after the old one loses power. Up to one Pi can be down at
a time and the cluster keeps working.

## Hardware and network

- Raspberry Pi OS **64-bit** (Lite is enough) on each Pi. Boot from a USB SSD
  if you can. Every confirmed message is fsynced on all in-sync nodes, so SD
  cards cap the throughput hard.
- One small switch. The three Pis and the laptop are all wired to it. Don't
  use Wi-Fi: booth Wi-Fi is the first thing to fail.
- Static IPs, e.g. `192.168.50.11`, `.12` and `.13` for the Pis and `.10` for
  the laptop. Pull the **power** cable for the demo. Pulling the network
  cable works the same way, but the audience understands power better.

## 1. Build the arm64 binaries (once, on any machine with Docker)

The Raft backend isn't released yet, so build it from the PR branch. Pin the
commit you rehearse with, and don't rebuild the night before the show.

```sh
git clone https://github.com/cloudamqp/lavinmq.git && cd lavinmq
git checkout <pinned commit of the raft-election branch>
docker buildx build --platform linux/arm64 \
  -f packaging/static-build/Dockerfile --output=bin-arm64 .
# bin-arm64/lavinmq and bin-arm64/lavinmqctl are static: no libraries needed on the Pi
```

Cross-building runs under QEMU and takes a while. Building natively on a
Pi 5 with the same command is often faster.

## 2. Install on each Pi

Copy this `pi/` directory and the two binaries to every Pi. Generate the
shared clustering password **once** and use the same file on all three:

```sh
openssl rand -base64 32 > pi/clustering_password
cp bin-arm64/lavinmq bin-arm64/lavinmqctl pi/
scp -r pi pi@192.168.50.11:   # and .12, .13
```

Then on each Pi:

```sh
# pi-1, the very first time only:
sudo ./pi/setup.sh 1 192.168.50.11 192.168.50.12 192.168.50.13 --bootstrap
# pi-2 and pi-3:
sudo ./pi/setup.sh 2 192.168.50.11 192.168.50.12 192.168.50.13
sudo ./pi/setup.sh 3 192.168.50.11 192.168.50.12 192.168.50.13
```

`--bootstrap` lets pi-1 become the first leader of a cluster where no node
has election state yet. Once there's a leader, remove it so a wiped pi-1 can
never start a new, empty cluster of its own:

```sh
sudo sed -i '/^bootstrap = true/d' /etc/lavinmq/lavinmq.ini   # on pi-1
```

The change takes effect on the next restart. There's no need to restart now.

## 3. Create the demo user (once, on any Pi)

The default `guest` user only works from localhost. The laptop needs an
administrator, because `/api/cluster` is admin only:

```sh
sudo lavinmqctl add_user booth '<password>'
sudo lavinmqctl set_user_tags booth administrator
sudo lavinmqctl set_permissions booth '.*' '.*' '.*'
```

Users are replicated, so you only do this once. If `lavinmqctl` says the node
is a follower, run it on the leader instead.

## 4. Check it

```sh
lavinmqctl cluster_status        # leader, term, ISR, members
journalctl -u lavinmq -f         # logs
curl -s localhost:15692/metrics | grep raft_   # what the dashboard reads
```

All three members should be `voter` and `in_isr: true`.

## Recovering

- **A Pi was unplugged:** plug it back in. It boots, rejoins as a follower,
  syncs, and goes back into the ISR on its own. The dashboard shows
  "Syncing → In sync".
- **Two Pis are down:** there's no majority, so there's no leader. Bring one
  back.
- **The cluster won't elect a leader after a messy day:** see "Recovering
  when no ISR member can come back" in `docs/clustering.md` on the PR branch.
- **Leaders keep changing without anyone touching the cables:** this is
  usually a slow SD card. Check `raft_storage_save_duration_seconds` in the
  metrics. Moving to SSDs fixes it. As a stopgap, raise `election_timeout` in
  `[clustering]` to 3000.
