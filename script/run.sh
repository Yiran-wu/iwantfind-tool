#!/bin/bash

spark-submit  --class com.iwantfind.Analyser  /Users/renbaoling/Documents/workspace/iwantfind-tool/target/iwantfind-tool-0.1-SNAPSHOT.jar   /Users/renbaoling/Documents/workspace/iwantfind-tool/target/


spark-submit --master yarn --deploy-mode client --name type   --class com.iwantfind.ASAnalyser --num-executors 10 --executor-memory 2g  /tmp/iwantfind-tool-0.1-SNAPSHOT.jar /testdata