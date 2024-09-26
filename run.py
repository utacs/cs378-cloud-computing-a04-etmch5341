#!/usr/bin/env python3

import argparse
from dataclasses import dataclass
import os
import subprocess

DATA_DIR = 'data'

@dataclass
class DataFileInfo:
    name: str
    url: str

DATA_FILES = {
    "small-https" : DataFileInfo("small-https.csv.bz2", "https://storage.googleapis.com/cs378/taxi-data-sorted-small.csv"),
    "small-gs" : DataFileInfo("small-gs.csv.bz2", "gs://cs378/taxi-data-sorted-small.csv"),
    "big": DataFileInfo("big.csv.bz2", "gs://cs378/taxi-data-sorted-large.csv"),
}

def ensure_data_file(data_id: str):
    # check that the directory exists
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
        
    # get the data file info
    data_file_info = DATA_FILES[data_id]
    
    # check that the compressed file exists
    file_path = os.path.join(DATA_DIR, data_file_info.name)
    if not os.path.exists(file_path):
        # download the file
        subprocess.run(['wget', '-O', file_path, data_file_info.url])

def run(data_id: str, cleansed_folder: str, intermediate_folder: str, task1_folder: str, task2_folder: str, task3_folder: str, rjar: bool):
    # ensure the data file
    ensure_data_file(data_id)
    
    # get the data file path
    data_file_path = os.path.join(DATA_DIR, DATA_FILES[data_id].name)
    
    # build the jar
    subprocess.run(['mvn', 'clean', 'package'])
    jar_file_path = 'target/MapReduce-WordCount-example-0.1-SNAPSHOT-jar-with-dependencies.jar'

    # run the code
    if rjar:
        subprocess.run(['java', '-jar', jar_file_path, data_file_path, cleansed_folder, intermediate_folder, task1_folder, task2_folder, task3_folder])
    else:
        # run as hadoop
        pass

def main():
    # create the parser
    parser = argparse.ArgumentParser(description='run the code')
    
    # add the args
    parser.add_argument('data_id', type=str, help='the data file to use', choices=DATA_FILES.keys())
    parser.add_argument('cleansed_folder', type=str, help='the cleansed folder to use')
    parser.add_argument('intermediate_folder', type=str, help='the intermediate folder to use')
    parser.add_argument('task1_folder', type=str, help='the task1 folder to use')
    parser.add_argument('task2_folder', type=str, help='the task2 folder to use')
    parser.add_argument('task3_folder', type=str, help='the task3 folder to use')

    # optional args
    parser.add_argument('--rjar', action='store_true', help='run the code as a jar')
                        
    # parse arguments
    args = parser.parse_args()
    
    # call the function
    run(args.data_id, args.cleansed_folder, args.intermediate_folder, args.task1_folder, args.task2_folder, args.task3_folder, args.rjar)

if __name__ == '__main__':
    main()