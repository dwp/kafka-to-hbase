#!/bin/bash

ADVERTISED_HOSTNAME=$(hostname -i)

exec /entrypoint.old.sh "$@"