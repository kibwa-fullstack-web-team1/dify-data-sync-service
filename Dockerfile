FROM python:3.12-slim

WORKDIR /app

# Install system dependencies if any (none for now)

# Copy requirements and install Python dependencies
COPY dify-data-sync-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY dify-data-sync-service/app ./app
COPY dify-data-sync-service/dify-data-sync-service_manage.py .

# Grant execute permissions to the manage script
RUN chmod +x dify-data-sync-service_manage.py

# Expose the service port
EXPOSE 8005

# Command to run the application
ENTRYPOINT ["python", "dify-data-sync-service_manage.py"]
