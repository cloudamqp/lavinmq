<img src="avalanche.png" align="right" />

# AvalancheMQ

A message queue server that implements the AMQP 0-9-1 protocol.
Written in [Crystal](https://crystal-lang.org/).

Aims to be very fast, have low RAM requirements, handle extremly long queues,
many connections and require minimal configuration.

## Installation

In Debian/Ubuntu:

```
cat > /etc/apt/sources.list.d/avalanchemq.list << EOF
deb [trusted=yes] https://apt.avalanchemq.com stable main
EOF

apt update
apt install avalanchemq
```

From source:

```
git clone git@github.com:avalanchemq/avalanchemq.git
cd avalanchemq
make
cp bin/avalanchemq /usr/bin/avalanchemq
```

Refer to
[Crystal's installation documentation](https://crystal-lang.org/docs/installation/)
on how to install Crystal.

## Usage

AvalancheMQ only requires one argument, and it's a path to a data directory:

`avalanchemq -D /var/lib/avalanchemq`

## Contributing

Fork, create feature branch, submit pull request.

## Contributors

- [Carl Hörberg](carl@84codes.com)
- [Anders Bälter](anders@84codes.com)

## License

The software is licensed under the Apache 2.0 license.

Copyright 2018 84codes AB

AvalancheMQ is a trademark of 84codes AB
