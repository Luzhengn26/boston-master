# syntax = docker/dockerfile:1.5
FROM artifactory.cd-tech26.de/docker/n26/python3.9:26ec8b AS main

WORKDIR /app
COPY src/utils/notebook_requirements.txt .

RUN pip install --upgrade pip==22.1.2
RUN --mount=type=secret,id=n26_pip_index_url \
  PIP_EXTRA_INDEX_URL="$(cat /run/secrets/n26_pip_index_url)" \
  python -m pip install \
  --no-cache-dir \
  -r notebook_requirements.txt

COPY . /app
WORKDIR /app
ENV PATH /app:$PATH
USER root
RUN chmod -R 777 .

FROM main AS format
RUN python -m pip install black[jupyter]
RUN black . --check && echo '\n--- Black Linter Completed ✅ ---' || { echo '\n--- Black Linter Failed Check 💀 ---\n'; exit 1; }
