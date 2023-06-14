# Use an official Python runtime as the base image
FROM python:3.9
# Set the working directory in the container
WORKDIR /app

# Copy the Python script into the container
COPY create.py /app

# Install the required dependencies
RUN pip install psycopg2

# Run the Python script when the container starts
CMD ["python", "create.py"]
