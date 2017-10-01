#!/usr/bin/env bash

# Check for data folder
if [ ! -d "4224-project-files" ]; then
    echo "Data folder not found"
    exit
fi

touch log.txt

cat 4224-project-files/xact-files/* | python app/Client.py 2>&1 | tee -a log.txt
