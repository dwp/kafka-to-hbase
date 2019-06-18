#!/bin/bash

ADVERTISED_HOSTNAME=$(hostname -i)

/entrypoint.old.sh "$@" &
PID=$!

trap "kill $PID" SIGINT SIGTERM

RETRIES=30
while [[ RETRIES > 0 ]]
do
    TOPICS=$(kafka-topics.sh --zookeeper localhost:2181 --list)
    if [ $? == 0 ]
    then
        break
    fi
    RETRIES=$((RETRIES-1))
done

if [[ $RETRIES == 0 ]]
then
    echo "Failed to list topics"
    exit 1
fi

if [[ ! " ${array[TOPICS]} " =~ " integration-test " ]]
then
    kafka-topics.sh --zookeeper localhost:2181 --create --topic integration-test --partitions 10 --replication-factor 1
fi

wait