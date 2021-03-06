#!/bin/sh

set -e

# If a proxy is requested, set it up

if [ -n "${INTERNET_PROXY}" ]; then
    export http_proxy="http://${INTERNET_PROXY}:3128"
    export HTTP_PROXY="http://${INTERNET_PROXY}:3128"
    export https_proxy="http://${INTERNET_PROXY}:3128"
    export HTTPS_PROXY="http://${INTERNET_PROXY}:3128"
    export no_proxy=169.254.169.254,.s3.eu-west-2.amazonaws.com,s3.eu-west-2.amazonaws.com,secretsmanager.eu-west-2.amazonaws.com
    export NO_PROXY=169.254.169.254,.s3.eu-west-2.amazonaws.com,s3.eu-west-2.amazonaws.com,secretsmanager.eu-west-2.amazonaws.com
    echo "Using proxy ${INTERNET_PROXY}"
fi

# Generate a cert for Kafka mutual auth
HOSTNAME=$(hostname)
if [ "${K2HB_KAFKA_INSECURE}" != "true" ]
then

    SSL_DIR="$(mktemp -d)"
    export K2HB_PRIVATE_KEY_PASSWORD="$(uuidgen)"

    export K2HB_KEYSTORE_PATH="${SSL_DIR}/k2hb.keystore"
    export K2HB_KEYSTORE_PASSWORD="$(uuidgen)"

    export K2HB_TRUSTSTORE_PATH="${SSL_DIR}/k2hb.truststore"
    export K2HB_TRUSTSTORE_PASSWORD="$(uuidgen)"

    if [ "${K2HB_KAFKA_CERT_MODE}" = "CERTGEN" ]; then

        echo "Generating cert for host ${HOSTNAME}"

        acm-pca-cert-generator \
            --subject-cn "${HOSTNAME}" \
            --keystore-path "${K2HB_KEYSTORE_PATH}" \
            --keystore-password "${K2HB_KEYSTORE_PASSWORD}" \
            --private-key-password "${K2HB_PRIVATE_KEY_PASSWORD}" \
            --truststore-path "${K2HB_TRUSTSTORE_PATH}" \
            --truststore-password "${K2HB_TRUSTSTORE_PASSWORD}"

        echo "Cert generation result is $? for ${HOSTNAME}"

    elif [ "${K2HB_KAFKA_CERT_MODE}" = "RETRIEVE" ]; then

        echo "Retrieving cert from ${RETRIEVER_ACM_CERT_ARN}"

        export RETRIEVER_ACM_KEY_PASSPHRASE="$(uuidgen)"

        acm-cert-retriever \
            --acm-key-passphrase "${RETRIEVER_ACM_KEY_PASSPHRASE}" \
            --keystore-path "${K2HB_KEYSTORE_PATH}" \
            --keystore-password "${K2HB_KEYSTORE_PASSWORD}" \
            --private-key-password "${K2HB_PRIVATE_KEY_PASSWORD}" \
            --truststore-path "${K2HB_TRUSTSTORE_PATH}" \
            --truststore-password "${K2HB_TRUSTSTORE_PASSWORD}"

        echo "Cert retrieve result is $? for ${RETRIEVER_ACM_CERT_ARN}"

    else
        echo "K2HB_KAFKA_CERT_MODE must be one of 'CERTGEN,RETRIEVE' but was ${K2HB_KAFKA_CERT_MODE}"
        exit 1
    fi
else
    echo "Skipping cert generation for host ${HOSTNAME}"
fi

export METADATASTORE_TRUSTSTORE=./truststore.jks
export METADATASTORE_TRUSTSTORE_PASSWORD="$(uuidgen)"

keytool -noprompt -import \
  -file "$METADATA_TRUSTSTORE_CERTIFICATE" \
  -keystore ./truststore.jks \
  -storepass "$METADATASTORE_TRUSTSTORE_PASSWORD"

exec "${@}"
