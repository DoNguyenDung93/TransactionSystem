#!/usr/bin/env bash

# Check for data folder
if [ ! -d "4224-project-files" ]; then
    echo "Data folder does not exist. Downloading it from remote."
    wget http://www.comp.nus.edu.sg/~cs4224/4224-project-files.zip -O 4224-project-files.zip
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
mkdir data-files
time python app/DataTransformer.py -p 4224-project-files/data-files/ -o data-files/

echo "Setting up models for Cassandra"
time cqlsh -f Schema_Commands.txt
echo "Done loading data"
