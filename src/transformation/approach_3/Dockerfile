# Base image
FROM openjdk:8-jdk

# Install Python and pip
RUN apt-get update && \
    apt-get install -y python3 python3-pip

# Set environment variables
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Set working directory
WORKDIR /app

ENV URL_LINK="https://xloop-dummy.herokuapp.com/"
# Copy the Python script and data file to the working directory
COPY transform_data.py /app/transform_data.py

# Install PostgreSQL development libraries
RUN apt-get install -y libpq-dev

# Install Python packages
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

# Download PostgreSQL JDBC driver
RUN wget -P /app https://jdbc.postgresql.org/download/postgresql-42.2.24.jar

# Execute the scheduler file
CMD ["spark-submit", "--master", "local", "--jars", "/app/postgresql-42.2.24.jar", "transform_data.py"]
