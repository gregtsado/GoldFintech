FROM apache/airflow:2.9.2-python3.9

# Set environment variables for Java and Spark
ENV JAVA_HOME=/opt/java/openjdk
ENV SPARK_HOME=/opt/spark
ENV PATH="$PATH:$JAVA_HOME/bin:$SPARK_HOME/bin:$SPARK_HOME/sbin"

# Copy the requirements file
COPY requirements.txt /requirements.txt

# Upgrade pip and install Python packages
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r /requirements.txt

# Install Java (required for Spark)
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-11-jdk-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Download and install Spark
RUN curl -o /tmp/spark.tgz \
    https://downloads.apache.org/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz && \
    mkdir -p $SPARK_HOME && \
    tar -xzf /tmp/spark.tgz -C $SPARK_HOME --strip-components=1 && \
    rm /tmp/spark.tgz

# Ensure Spark's PySpark is accessible
RUN pip install pyspark

# Expose necessary ports for Spark UI (optional)
EXPOSE 4040 8080 7077

# Set the working directory
WORKDIR /opt/airflow
