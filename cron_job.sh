#!/bin/bash
# Generate random message
python generate_random_message.py

#put to hdfs
hdfs dfs -put -f message_test.txt /user/baur/output/

#run yarn job
yarn jar target/lab1-1.0-SNAPSHOT-jar-with-dependencies.jar /user/baur/LICENSE.txt "/output_$(date + %Y%m%d_%H%M%S)"

