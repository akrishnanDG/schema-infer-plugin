FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml README.md ./
COPY schema_infer/ schema_infer/

RUN pip install --no-cache-dir .

ENTRYPOINT ["schema-infer"]
CMD ["--help"]
