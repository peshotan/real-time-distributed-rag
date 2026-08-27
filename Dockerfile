FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    HF_HOME=/models

WORKDIR /app

RUN useradd --create-home --uid 10001 appuser
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY common ./common
COPY gateway ./gateway
COPY ingestion ./ingestion
COPY processing ./processing
COPY scripts ./scripts

RUN mkdir -p /models && chown -R appuser:appuser /app /models
USER appuser

CMD ["python", "-m", "gateway.main"]