FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (layer-cache friendly)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .

# Run as non-root
RUN useradd --no-create-home appuser
USER appuser

CMD ["python", "-u", "producer.py"]
