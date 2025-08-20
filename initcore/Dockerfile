FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY init_core_tables.py ./

CMD ["python", "init_core_tables.py"]
