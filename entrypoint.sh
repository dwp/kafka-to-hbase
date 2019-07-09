#!/bin/bash

set -e

# Create a user to run the process as instead of root

: ${SUID:=1000}
: ${SGID:=1000}

if ! getent passwd user > /dev/null
then
    groupadd --non-unique --gid "$SGID" user
    useradd --create-home --no-user-group --non-unique --uid "$SUID" --gid "$SGID" user
fi

# If a proxy is requested, set it up

if [ "${INTERNET_PROXY}" ]; then
  export http_proxy="http://${INTERNET_PROXY}:3128"
  export https_proxy="https://${INTERNET_PROXY}:3128"
  export no_proxy=169.254.169.254
  echo "Using proxy ${INTERNET_PROXY}"
fi

# Generate a cert for Kafka mutual auth

if [[ ${KAFKA_INSECURE} != "true" ]]
then
    echo "Generating cert for host ${HOSTNAME}"
    acm-pca-cert-generator --subject-cn "${HOSTNAME}"
    echo "Cert generation result for ${HOSTNAME} is: $?"
else
    echo "Skipping cert generation for host ${HOSTNAME}"
fi

chown ${SUID}:${SGID} . -R
exec gosu user "${@}"
