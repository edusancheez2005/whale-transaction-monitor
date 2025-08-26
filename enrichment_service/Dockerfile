FROM python:3.10-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Expose API port
EXPOSE 8000

# Command to run the application
CMD ["uvicorn", "enrichment_service.api.main:app", "--host", "0.0.0.0", "--port", "8000"] 