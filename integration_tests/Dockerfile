FROM python:3.7.3

WORKDIR /tests

COPY requirements.txt .

RUN pip3 install -r requirements.txt

ENTRYPOINT ["behave"]