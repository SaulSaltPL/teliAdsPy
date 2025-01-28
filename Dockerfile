FROM python:3.9-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY teliads.py .
COPY passkeys.json .
COPY zeta-environs-448616-m0-cb4f0707f662.json .

EXPOSE 8080
ENV PORT=8080


# Simpler gunicorn configuration with increased timeout
CMD exec gunicorn \
    --bind :$PORT \
    --workers 1 \
    --threads 8 \
    --timeout 0 \
    --log-level debug \
    --capture-output \
    --access-logfile - \
    --error-logfile - \
    teliads:app