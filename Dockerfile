FROM airbyte/source-declarative-manifest:6.54.6

COPY manifest.yaml /airbyte/integration_code/source_declarative_manifest/manifest.yaml
COPY components.py /airbyte/integration_code/source_declarative_manifest/components.py
COPY main_custom.py /airbyte/integration_code/main_custom.py

ENV AIRBYTE_ENTRYPOINT="python /airbyte/integration_code/main_custom.py"
ENTRYPOINT ["python", "/airbyte/integration_code/main_custom.py"]
