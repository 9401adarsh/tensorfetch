#!/bin/bash
FILE=$1
ITER=${2:-1000}

if [ -z "$FILE" ]; then
  echo "Usage: ./open_loop.sh <file> [iterations]"
  exit 1
fi

for i in $(seq 1 $ITER); do
  cat "$FILE" > /dev/null
  sleep 0.05
  ((i % 100 == 0)) && echo "Opened $i times"
done