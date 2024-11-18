FROM python:3.10.12

WORKDIR /app

RUN apt-get update && apt-get install -y default-jre

RUN pip install pyspark deduce pandas venv-pack pyarrow

COPY app/pyspark_deducer.py /app
RUN mkdir -p /data/input
RUN mkdir -p /data/output

EXPOSE 4040

ENTRYPOINT ["python", "pyspark_deducer.py"]
#CMD ["--input_file input.csv"]


## Use following commands in terminal to build and run container
## Build image from this Dockerfile (replace latest with other tag value if you want)

#docker build -t pyspark_deducer_app:latest .

## Run the image
## The first -v makes/connects a Docker Volume to the /data folder within the container 
## A Docker Volume is disk space managed by docker to be shared by host and container. This avoids unexpected changes to other host disk locations.
## The second -v connects your local folder with input data to the /data/input folder in the container. Instead of a folder an individual file should also work.
## Note that ${pwd} notation is powershell specific, try out what works in your terminal or use full file path
## pyspark_deducer_app is the name of the image, :latest indicates a tag
## input.csv is an argument for the code execution, that will be added to the CMD command above.
## Replace arguments with /bin/bash and add -it to run command to avoid entering the program and open a terminal inside the container.

##Normal, simple situation:

#docker run -v deducerVol:/data -v ${pwd}/data/input:/data/input  pyspark_deducer_app input.csv

## Interactive terminal, binding down to a specific file (second -v), calling a specific image version using :latest tag

#docker run -it -v deducerVol:/data -v ${pwd}/data/input.csv:/data/input/input.csv --entrypoint /bin/bash pyspark_deducer_app:latest

## For help with the python script

#docker run --entrypoint python carmenda/pseudonymizer:latest pyspark_deducer.py --help
