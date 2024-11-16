FROM apache/airflow:2.9.2-python3.9

# Copy requirement file

COPY requirements.txt /requirements.txt


# upgrade pip
RUN pip install --upgrade pip

# install packages

RUN pip install --no-cache-dir -r /requirements.txt