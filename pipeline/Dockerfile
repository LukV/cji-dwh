# Use the official Python 3.11.7 slim image
FROM python:3.11.7-slim

# Set environment variables
ENV DAGSTER_HOME=/opt/dagster
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/opt/code

# Create the DAGSTER_HOME directory
RUN mkdir -p $DAGSTER_HOME

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /opt/code

# Copy the requirements.txt from the root of the monorepo
COPY ../requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Copy dagster.yaml into DAGSTER_HOME
COPY dagster.yaml $DAGSTER_HOME/dagster.yaml

# Expose ports for the webserver and gRPC
EXPOSE 3000 4000

# Install supervisor to manage multiple processes
RUN pip install supervisor

# Create a supervisor configuration directory
RUN mkdir -p /etc/supervisor/conf.d

# Copy the supervisor configuration
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Set the command to start supervisord
CMD ["supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]
