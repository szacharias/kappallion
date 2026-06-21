FROM python:3.11-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir kafka-python-ng

# Copy source code
COPY simulator.py .

CMD ["python", "payload_simulator.py"]
