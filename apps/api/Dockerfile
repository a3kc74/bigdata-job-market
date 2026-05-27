FROM python:3.11-slim

WORKDIR /app

COPY apps/api/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY apps/api /app/apps/api

ENV PYTHONPATH=/app

EXPOSE 8000

CMD ["uvicorn", "apps.api.search_api:app", "--host", "0.0.0.0", "--port", "8000"]
