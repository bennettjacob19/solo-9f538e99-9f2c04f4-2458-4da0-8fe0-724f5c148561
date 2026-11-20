FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1
WORKDIR /app

COPY receiver.py .

EXPOSE 9001/udp 9101/udp 9201/tcp
CMD ["python", "receiver.py"]
