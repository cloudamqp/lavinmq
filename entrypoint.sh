#!/bin/bash
ENV_ARGS=""

if [ -n "${LAVINMQ_CONF}" ]; then
  ENV_ARGS="--config=$LAVINMQ_CONF"
fi

if [ -n "${LAVINMQ_DATADIR}" ]; then
  ENV_ARGS="$ENV_ARGS --data-dir=$LAVINMQ_DATADIR"
fi

if [ -n "${LAVINMQ_BIND}" ]; then
  ENV_ARGS="$ENV_ARGS --bind=$LAVINMQ_BIND"
fi

if [ -n "${LAVINMQ_PORT}" ]; then
  ENV_ARGS="$ENV_ARGS --amqp-port=$LAVINMQ_PORT"
fi

if [ -n "${LAVINMQ_AMQPS_PORT}" ]; then
  ENV_ARGS="$ENV_ARGS --amqps-port=$LAVINMQ_AMQPS_PORT"
fi

if [ -n "${LAVINMQ_AMQP_BIND}" ]; then
  ENV_ARGS="$ENV_ARGS --amqp-bind=$LAVINMQ_AMQP_BIND"
fi

if [ -n "${LAVINMQ_HTTP_PORT}" ]; then
  ENV_ARGS="$ENV_ARGS --http-port=$LAVINMQ_HTTP_PORT"
fi

if [ -n "${LAVINMQ_HTTPS_PORT}" ]; then
  ENV_ARGS="$ENV_ARGS --https-port=$LAVINMQ_HTTPS_PORT"
fi

if [ -n "${LAVINMQ_HTTP_BIND}" ]; then
  ENV_ARGS="$ENV_ARGS --http-bind=$LAVINMQ_HTTP_BIND"
fi

if [ -n "${LAVINMQ_AMQP_UNIX_PATH}" ]; then
  ENV_ARGS="$ENV_ARGS --amqp-unix-path=$LAVINMQ_AMQP_UNIX_PATH"
fi

if [ -n "${LAVINMQ_HTTP_UNIX_PATH}" ]; then
  ENV_ARGS="$ENV_ARGS --http-unix-path=$LAVINMQ_HTTP_UNIX_PATH"
fi

if [ -n "${LAVINMQ_CERT}" ]; then
  ENV_ARGS="$ENV_ARGS --cert $LAVINMQ_CERT"
fi

if [ -n "${LAVINMQ_KEY}" ]; then
  ENV_ARGS="$ENV_ARGS --key $LAVINMQ_KEY"
fi

if [ -n "${LAVINMQ_CIPHERS}" ]; then
  ENV_ARGS="$ENV_ARGS --ciphers $LAVINMQ_CIPHERS"
fi

if [ -n "${LAVINMQ_TLS_MIN_VERSION}" ]; then
  ENV_ARGS="$ENV_ARGS --tls-min-version=$LAVINMQ_TLS_MIN_VERSION"
fi

if [ -n "${LAVINMQ_LOG_LEVEL}" ]; then
  ENV_ARGS="$ENV_ARGS --log-level=$LAVINMQ_LOG_LEVEL"
fi

if [ -n "${LAVINMQ_RAISE_GC_WARN}" ]; then
  if [ "$LAVINMQ_RAISE_GC_WARN" = "true" ]; then
    ENV_ARGS="$ENV_ARGS --raise-gc-warn"
  fi
fi

if [ -n "${LAVINMQ_NO_DATA_DIR_LOCK}" ]; then
  if [ "$LAVINMQ_NO_DATA_DIR_LOCK" = "true" ]; then
    ENV_ARGS="$ENV_ARGS --no-data-dir-lock"
  fi
fi

if [ -n "${LAVINMQ_DEBUG}" ]; then
  if [ "$LAVINMQ_DEBUG" = "true" ]; then
    ENV_ARGS="$ENV_ARGS --debug"
  fi
fi

if [ -n "${LAVINMQ_GUEST_ONLY_LOOPBACK}" ]; then
    ENV_ARGS="$ENV_ARGS --guest-only-loopback=$LAVINMQ_GUEST_ONLY_LOOPBACK"
fi

exec lavinmq "$@ $ENV_ARGS"
