FROM artifactory.cd-tech26.de/docker/n26/python3.9:4be5f6

WORKDIR /app

COPY requirements.txt .

ARG PIP_EXTRA_INDEX_URL
RUN pip install --upgrade pip==22.1.2

RUN pip install --no-cache-dir --upgrade --upgrade-strategy=eager -r requirements.txt

COPY . /app

WORKDIR /app
ENV PATH /app:$PATH

USER root

EXPOSE 8000

RUN chmod -R 777 .

CMD python -u setup/frontend/frontend_transfer.py
