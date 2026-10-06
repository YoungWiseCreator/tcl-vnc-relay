FROM python:3.12-slim
WORKDIR /app
COPY relay.py .
CMD ["python", "relay.py"]
