#!/bin/bash

function wait_for() {
    CMD="${@}"
    RETRIES=30
    while [[ ${RETRIES} > 0 ]]
    do
        if ${CMD}
        then
            return
        fi
        : $((RETRIES--))
    done
    echo "Failed to successfully run ${CMD} after 30 attempts"
    exit 1
}


set -e

: ${JAVA_HOME:=/usr}
export JAVA_HOME

: ${ZOOKEEPER_QUORUM:=localhost}
: ${ZOOKEEPER_PORT:=2181}
sed -e "s/{{ZOOKEEPER_PORT}}/${ZOOKEEPER_PORT}/" \
    -e "s/{{ZOOKEEPER_QUORUM}}/${ZOOKEEPER_QUORUM}/" \
    /hbase/conf/hbase-site.template.xml > /hbase/conf/hbase-site.xml

if [[ -t 0 || $1 == "shell" ]]
then
    # Running interactively so assume we are trying to run an Hbase command
    exec /hbase/bin/hbase "${@}"
fi

pgrep -f proc_rest && pkill -9 -f proc_rest

echo "*** Starting HBase ***"
/hbase/bin/start-hbase.sh

: ${HBASE_SCRIPT:=}
if [[ -n ${HBASE_SCRIPT} ]]
then
    echo "Waiting for Hbase to start up"
    wait_for hbase zkcli close || :
    wait_for hbase canary -t 1000 -f true || :

    echo "Running Hbase initialisation script"
    echo "${HBASE_SCRIPT}"
    echo "${HBASE_SCRIPT}" | hbase shell
fi

trap_func() {
    echo -e "*** Shutting down HBase ***"
    /hbase/bin/local-regionservers.sh stop 1 || :
    /hbase/bin/stop-hbase.sh
    sleep 2
    ps -ef | grep org.apache.hadoop.hbase | grep -v -i org.apache.hadoop.hbase.zookeeper | awk '{print $1}' | xargs kill 2>/dev/null || :
    sleep 3
    pkill -f org.apache.hadoop.hbase.zookeeper 2>/dev/null || :
    sleep 2
}
trap trap_func INT QUIT TRAP ABRT TERM EXIT

tail -f /dev/null /hbase/logs/* &

wait || :