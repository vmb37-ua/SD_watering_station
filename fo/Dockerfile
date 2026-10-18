FROM python:3.12-slim
WORKDIR /app

LABEL name="field-operator"

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONBUFFERED=1
EXPOSE 8000

ENTRYPOINT ["python", "-u", "WM_FO.py"]

# CMD sirve para añadir argumentos por defecto al entrypoint si el usuario no da ninguno
CMD ["--help"]

