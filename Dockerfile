FROM python:3 as build

WORKDIR /app

RUN pip3 install --upgrade pip setuptools wheel

# Copy and install requirements first so changes in code don't cause a full rebuild
COPY requirements.txt .
COPY requirements-dev.txt .
RUN pip3 install -r requirements.txt -r requirements-dev.txt

# Copy and install the package
COPY . .
RUN python3 setup.py bdist_wheel

FROM python:3-slim

WORKDIR /dist

COPY --from=build /app/dist/kafka_to_hbase-1.0-py3-none-any.whl .

# Install the required build tooling, then the package, then remove build-only tooling
RUN buildDeps='build-essential python3-dev' && \
    set -x && \
    apt-get update && apt-get install -y $buildDeps --no-install-recommends && \
    pip3 install ./kafka_to_hbase-1.0-py3-none-any.whl && \
    rm -rf /var/lib/apt/lists/* && \
    apt-get purge -y --auto-remove $buildDeps

CMD ["kafka-to-hbase"]