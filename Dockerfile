###
# Simple intermediate image to package the wheel
###
FROM python:3 as build

WORKDIR /app

RUN pip3 install --upgrade pip setuptools wheel

# Copy and install the package
COPY . .
RUN python3 setup.py bdist_wheel

###
# Final image building on install_deps
FROM python:3-slim

WORKDIR /tmp

# Install the required build tooling, then the package
RUN buildDeps='build-essential python3-dev' && \
    set -x && \
    apt-get update && apt-get install -y $buildDeps --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*

# Copy and install requirements first so changes in code don't cause a full rebuild
COPY requirements.txt .
COPY requirements-dev.txt .
RUN pip3 install -r requirements.txt -r requirements-dev.txt

# Copy and install the actual package
COPY --from=build /app/dist/kafka_to_hbase-1.0-py3-none-any.whl .
RUN pip3 install ./kafka_to_hbase-1.0-py3-none-any.whl

CMD ["kafka-to-hbase"]