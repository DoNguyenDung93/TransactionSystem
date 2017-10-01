#!/usr/bin/env bash

# Check for data folder
if [ ! -d "4224-project-files" ]; then
    echo "Data folder not found"
    exit
fi

touch log.txt

python app/Client.py < `cat 4224-project-files/xact-files/*` 2>&1 | tee -a log.txt
