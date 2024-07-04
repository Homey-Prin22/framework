#!/bin/bash

# Infinite loop to execute the tasks every 60 seconds
while true; do
  # Add a character "x" to the file abc.txt
  echo -n "x" >> text/file.txt

  # Stage all changes for commit
  git add *

  # Commit the changes with a message
  git commit -m "Updated file.txt"

  # Push the changes to the main branch
  git push origin main

  # Wait for 60 seconds before the next iteration
  sleep 60
done

