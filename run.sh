# control vars
DATA_ID=big
CLEANSED="cleansed"
INTERMEDIATE="intermediate"
TASK1="task1"
TASK2="task2"
TASK3="task3"

RJAR=""
# RJAR="--rjar"

# delete all the folders
rm -rf $CLEANSED
rm -rf $INTERMEDIATE
rm -rf $TASK1
rm -rf $TASK2
rm -rf $TASK3

# exec 
python3 ./run.py $RJAR $DATA_ID $CLEANSED $INTERMEDIATE $TASK1 $TASK2 $TASK3