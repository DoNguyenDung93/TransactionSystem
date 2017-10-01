#!/usr/bin/env bash

# Check for data folder
if [ ! -d "4224-project-files" ]; then
    echo "Data folder not found"
    exit
fi

touch log.txt

for f in `ls 4224-project-files/xact-files`; do
    python app/Client.py < "4224-project-files/xact-files/${f}" 2>&1 | tee -a log.txt
done
