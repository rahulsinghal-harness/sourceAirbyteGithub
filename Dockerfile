FROM python:3.11-slim

WORKDIR /connector

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV AIRBYTE_ENTRYPOINT="python /connector/main.py"
ENTRYPOINT ["python", "/connector/main.py"]
