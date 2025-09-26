# Start from official python image
FROM python:3.11-slim

WORKDIR /app

# system deps for psycopg2
RUN apt-get update && apt-get install -y --no-install-recommends gcc libpq-dev && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY bot_full.py .
COPY migrate_sqlite_to_neon.py .

ENV PYTHONUNBUFFERED=1

CMD ["python", "bot_full.py"]