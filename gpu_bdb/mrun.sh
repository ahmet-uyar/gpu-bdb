#!/bin/bash

rfile="results.txt"

rm $rfile

for n in {1..5}; do
  echo "Running $n"
  ./run.sh $rfile
done
