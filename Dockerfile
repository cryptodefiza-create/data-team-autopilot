FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md /app/
COPY src /app/src
COPY scripts /app/scripts

RUN pip install --upgrade pip && pip install -e .[dev,live]

EXPOSE 8000

CMD ["uvicorn", "data_autopilot.main:app", "--host", "0.0.0.0", "--port", "8000"]
