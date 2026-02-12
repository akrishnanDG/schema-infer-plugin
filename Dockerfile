FROM python:3.9-slim

WORKDIR /app

COPY pyproject.toml requirements.txt ./
COPY schema_infer/ schema_infer/

RUN pip install --no-cache-dir .

ENTRYPOINT ["schema-infer"]
CMD ["--help"]
