FROM python:3.10-slim

WORKDIR /app

COPY shared /app/shared
COPY services/processing/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY services/processing/src /app/src

ENV PYTHONPATH=/app

CMD ["python", "-u", "src/transform.py"]
