FROM python:3.12-slim
WORKDIR /app

LABEL name="ws"

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1

# El comando no se pone aqui porque la misma imagen vale para el monitor y para el engine, se pone en el docker-compose
