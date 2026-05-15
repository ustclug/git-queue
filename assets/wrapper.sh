#!/bin/bash

if [ -z "$BASH_VERSION" ]; then
  echo "Error: This is not GNU Bash" >&2
  exit 1
fi

QUEUE_SERVER_HOST="${QUEUE_SERVER_HOST:-127.0.0.1}"
QUEUE_SERVER_PORT="${QUEUE_SERVER_PORT:-9419}"
exec 3<>"/dev/tcp/$QUEUE_SERVER_HOST/$QUEUE_SERVER_PORT" 2>/dev/null
if [ $? -ne 0 ]; then
  # Fail-open
  exec "$@"
fi

env >&3
echo % >&3

payload="$(cat)"
waited=0
while read -u 3 -r line; do
  case "$line" in
  0)
    break;;
  -1)
    echo "Queue is full, exiting." >&2
    exit 1;;
  esac
  printf "Waiting in queue... (Position: %s)\r" "$line" >&2
  waited=1
done
if [ "$waited" -ne 0 ]; then
  printf "\n" >&2
fi

# No "exec" here, keep socket alive until process exits
"$@" <<< "$payload" 3>&-
exit $?
