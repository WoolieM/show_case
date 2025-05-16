# syntax=docker/dockerfile:1

FROM python:3.13.3-alpine3.21

WORKDIR /app
COPY main.py /app/main.py
COPY fast_requirements.txt /app/fast_requirements.txt
RUN pip install --no-cache-dir -r /app/fast_requirements.txt

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]



