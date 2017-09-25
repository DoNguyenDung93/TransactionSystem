#!/usr/bin/env bash

# This script uses the sed command to replace all null values with -1 in all csv files.

for f in *.csv
do
    echo "Replacing null in ${f}"
	sed -i -e 's/,null,/,-1,/g' $f
	sed -i -e 's/^null,/-1,/' $f
	sed -i -e 's/-1,null,/-1,-1,/g' $f
	sed -i -e 's/,null$/,-1/' $f
done

