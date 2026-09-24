FROM python:3.12-slim
WORKDIR /app

LABEL name="central"

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1
EXPOSE 5000

ENTRYPOINT ["python", "-u", "script.py"]
CMD ["--port", "5000"]