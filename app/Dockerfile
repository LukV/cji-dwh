# Use the official Python 3.11.7 slim image
FROM python:3.11.7-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV API_KEY=01JC3T26KNVYFSXXW7R38B74GA

# Set the working directory
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY . .

# Expose the Streamlit port (default is 8501)
EXPOSE 8501

# Set the command to run the Streamlit app
CMD ["streamlit", "run", "src/app.py", "--server.port=8501", "--server.address=0.0.0.0"]
