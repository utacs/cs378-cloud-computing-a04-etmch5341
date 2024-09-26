
#TODO: run hadoop application through GC bucket

# control vars

DATA_FOLDER="data"
CHUNK_FOLDER="chunks"
RESULT_FOLDER="results"
INTERMEDIATE_FOLDER="intermediateFolder"

DATA_URL="https://storage.googleapis.com/cs378/taxi-data-sorted-small.csv"
DATA_NAME="taxi-data-sorted-small.csv"

CHUNK_N_CHARS=100000000

INTERMEDIATE_NAME="intermediate.txt"

RESULT_NAME="SORTED-FILE-RESULT.txt"

#Java
JAR_FILE="MapReduce-WordCount-example-0.1-SNAPSHOT-jar-with-dependencies.jar"

#Hadoop
JAR_FILE_H="MapReduce-WordCount-example-0.1-SNAPSHOT"
MAIN_CLASS="edu.cs.utexas.HadoopEx.TaxiDataDriver"
GS_INPUT="gs://your-gcs-bucket/path/to/input"
GS_OUTPUT="gs://your-gcs-bucket/path/to/output"

# setup environmnent

mkdir -p $DATA_FOLDER
mkdir -p $CHUNK_FOLDER
mkdir -p $RESULT_FOLDER
mkdir -p $INTERMEDIATE_FOLDER

if [ ! -f "$DATA_FOLDER/$DATA_NAME" ]; then
    wget $DATA_URL -O $DATA_FOLDER/$DATA_NAME
fi

# building
#mvn clean
#mvn clean compile exec:java -Dexec.executable="edu.utexas.cs.cs378.Main"  -Dexec.args="$DATA_FOLDER/$DATA_NAME $CHUNK_N_CHARS $CHUNK_FOLDER $RESULT_FOLDER/$RESULT_NAME"


#NEW

# Run Locally
mvn clean package
#java -jar -Dexec.args="$JAR_FILE $DATA_FOLDER/$DATA_NAME $INTERMEDIATE_FOLDER/$INTERMEDIATE_NAME $RESULT_FOLDER/$RESULT_NAME"

#Run on cloud using hadoop application
#hadoop jar -Dexec.args "$JAR_FILE_H $MAIN_CLASS $GS_INPUT $GS_OUTPUT"
