#!usr/bin/env bash

#TODO: run hadoop application through GC bucket

# control vars

DATA_FOLDER="data"
CHUNK_FOLDER="chunks"
RESULT_FOLDER="results"

DATA_URL="https://www.cs.utexas.edu/~kiat/datasets/taxi-data-sorted-small.csv.bz2"
# DATA_URL="https://www.cs.utexas.edu/~kiat/datasets/taxi-data-sorted-large-n.csv.bz2"
DATA_NAME="taxi-data.csv.bz2"

CHUNK_N_CHARS=100000000

RESULT_NAME="SORTED-FILE-RESULT.txt"

# setup environmnent

mkdir -p $DATA_FOLDER
mkdir -p $CHUNK_FOLDER
mkdir -p $RESULT_FOLDER

if [ ! -f "$DATA_FOLDER/$DATA_NAME" ]; then
    wget $DATA_URL -O $DATA_FOLDER/$DATA_NAME
fi

# building
mvn clean
mvn clean compile exec:java -Dexec.executable="edu.utexas.cs.cs378.Main"  -Dexec.args="$DATA_FOLDER/$DATA_NAME $CHUNK_N_CHARS $CHUNK_FOLDER $RESULT_FOLDER/$RESULT_NAME"


#NEW


# Run Locally
mvn clean package

hadoop jar
#hadoop jar your-application.jar [main-class] gs://your-gcs-bucket/path/to/input gs://your-gcs-bucket/path/to/output
