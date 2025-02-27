#!/bin/bash

# Path to the root project requirements.txt
ROOT_REQ_FILE="requirements.txt"

# Use find to locate all requirements.txt files in subdirectories
find . -type f -name "requirements.txt" | while read req_file; do
  # Skip the root requirements.txt itself
  if [[ "$req_file" != "./requirements.txt" ]]; then
    echo "Overriding $req_file"
    cp "$ROOT_REQ_FILE" "$req_file"
  fi
done

echo "All requirements.txt files have been overridden."
