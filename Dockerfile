FROM python:3.11-slim

WORKDIR /connector

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py source.py auth.py ./
COPY streams/ streams/
COPY ai_asset_auto_discovery/ ai_asset_auto_discovery/

ENV AIRBYTE_ENTRYPOINT="python /connector/main.py"
ENTRYPOINT ["python", "/connector/main.py"]
