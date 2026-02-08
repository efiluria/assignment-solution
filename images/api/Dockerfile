FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY images/api/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy source code
COPY src /app/src
COPY config /app/config

EXPOSE 8000

CMD ["uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
