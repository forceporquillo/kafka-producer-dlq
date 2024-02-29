#!/bin/bash

java -jar target/kafka-reproduce-example-1.0-SNAPSHOT-jar-with-dependencies.jar "$@"

if [ "$?" -eq "0" ]; then
  exit 0
fi
exit 1