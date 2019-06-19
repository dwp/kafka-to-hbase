#!/bin/bash

/entrypoint.old.sh "$@" &
PID=$!

trap "kill $PID" SIGINT SIGTERM

RETRIES=30
while [[ RETRIES > 0 ]]
do
    if ! (echo -e "list" | hbase shell 2>&1 | grep -q "ERROR:")
    then
        break
    fi
    RETRIES=$((RETRIES-1))
done

if [[ $RETRIES == 0 ]]
then
    echo "Failed to list tables"
    exit 1
fi

# Create the k2hb namespace and relevant tables
hbase shell -n <<'EOF'
describe_namespace 'k2hb' or create_namespace 'k2hb'
exists 'k2hb:integration-test' or create 'k2hb:integration-test', NAME => 'cf', VERSIONS => 10
exists 'k2hb:docker' or create 'k2hb:docker', NAME => 'cf', VERSIONS => 10
EOF

wait