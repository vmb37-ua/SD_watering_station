FROM python:3.12-slim
WORKDIR /app

LABEL name="central"

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1
EXPOSE 5000

CMD ["sh", "-c", "exec python -u central.py ${SOCKET_PORT:-5000} ${KAFKA_BOOTSTRAP:-broker:29092} --http-port ${PORT:-${HTTP_PORT:-8080}} --db ${DB_PATH:-/data/database.db}"]
