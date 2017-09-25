#!/usr/bin/env bash

# Check for data folder
if [ ! -d "4224-project-files" ]; then
    echo "Data folder does not exist. Downloading it from remote."
    wget http://www.comp.nus.edu.sg/~cs4224/4224-project-files.zip
    unzip 4224-project-files.zip
    rm 4224-project-files.zip
    echo "Data downloaded successfully!"
else
    echo "Data folder already exists"
fi

echo "Replacing null values"
cp replacenull.sh 4224-project-files/data-files/
cd 4224-project-files/data-files
./replacenull.sh
echo "Done replacing null values"
cd ../../

echo "Setting up data for Cassandra"
cqlsh -f Schema_Commands.txt
