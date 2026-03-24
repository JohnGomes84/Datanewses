FROM python:3.10-slim

WORKDIR /app

COPY shared /app/shared
COPY services /app/services
COPY scripts /app/scripts
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONPATH=/app

CMD ["python", "-u", "scripts/test_pipeline.py"]
