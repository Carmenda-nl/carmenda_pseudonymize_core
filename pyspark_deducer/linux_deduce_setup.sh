#These commands were drawn from a history of manual trial and error, it therefore serves as a basis for building the correct software environment (in a Docker image)
sudo apt-get update; sudo apt-get upgrade
sudo apt-get install python3-pip
sudo apt-get install python3-virtualenv

mkdir deduce
cd deduce
virtualenv deduce_venv
source deduce_venv/bin/activate

pip install pyspark deduce pandas venv-pack pyarrow

sudo apt-get install default-jre

venv-pack -o deduce_venv.tar.gz

export PYSPARK_DRIVER_PYTHON = python
export PYSPARK_PYTHON = python
pyspark --archives deduce_venv.tar.gz
