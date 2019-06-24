FROM python:3.7.3

WORKDIR /tests

COPY requirements.txt .

RUN pip3 install --trusted-host=files.pythonhosted.org --trusted-host=pypi.org -r requirements.txt

CMD ["nosetests"]