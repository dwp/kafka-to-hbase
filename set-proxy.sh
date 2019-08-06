#!/bin/bash

if [[ ! -z "$http_proxy" ]] then
    echo "Acquire::http::Proxy $http_proxy;" > /etc/apt/apt.conf
    export http_proxy=${http_proxy_value}
    export https_proxy=${https_proxy_value}
    export HTTP_PROXY=${http_proxy_value}
    export HTTPS_PROXY=${https_proxy_value}
fi