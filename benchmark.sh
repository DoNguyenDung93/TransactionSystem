#!/usr/bin/env bash
# Experiment scripts to be run in sunfire node

# run experiment with NC argument supplied, e.g `./benchmark.sh 20`
NC=$1
LEVEL=1

for i in `seq 1 ${NC}`; do
    serverIdx=$(( 35 + $i % 5 ))
    log="log${i}.txt"
    rm -f $log
    touch $log
    ssh "cs4224h@xcnd${serverIdx}.comp.nus.edu.sg" \
     "cd TransactionSystem-master && python app/Client.py ${LEVEL} < 4224-project-files/xact-files/${i}.txt" \
     2>&1 | tee -a $log &
done