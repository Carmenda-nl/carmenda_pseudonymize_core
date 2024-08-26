FROM python:3.10.12

WORKDIR /app

RUN apt-get update && apt-get install -y default-jre

RUN pip install pyspark deduce pandas venv-pack pyarrow

COPY app/pyspark_deducer.py /app
RUN mkdir -p /data/input
RUN mkdir -p /data/output

COPY data/input/ECD_dummy_dataset_20240502_161537.csv /data/input/ECD_dummy_dataset_20240502_161537.csv

EXPOSE 4040

CMD ["python", "pyspark_deducer.py"]