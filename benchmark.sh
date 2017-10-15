#!/usr/bin/env bash
# Experiment scripts to be run in sunfire node

echo `date` >> duration.txt
# run experiment with NC argument supplied, e.g `./benchmark.sh 20`
NC=$1
LEVEL=$2
acc_arr=(`tail -1 config.txt`)

for i in `seq 1 ${NC}`; do
    serverIdx=$(( $i % 5 ))
    log="log${i}.txt"
    rm -f $log
    touch $log
    acc=${acc_arr[$(($serverIdx))]}
    ssh ${acc} \
     "cd TransactionSystem-master && python app/Client.py ${LEVEL} < 4224-project-files/xact-files/${i}.txt > /dev/null" \
     2>&1 | tee -a $log &
done

# Wait for all processes to finish and output final db states
wait
rm -f db_state.txt
touch db_state.txt
ssh ${acc_arr[0]} \
 "cd TransactionSystem-master && python app/FinalOutputer.py ${LEVEL}" 2>&1 | tee -a db_state.txt

echo `date` >> duration.txt

# Get aggregate stats

NR=`ls | grep log | wc -l`
sum=0.0
max=-1
min=10000

for f in `ls | grep log`; do
  next=`tail -1 $f | awk '{ print $3 }'`
  sum=`echo $sum $next | awk '{print $1 + $2}'`
  min=`echo $min $next | awk '{if($2<$1){print $2}else{print $1}}'`
  max=`echo $max $next | awk '{if($2>$1){print $2}else{print $1}}'`
done

avg=`echo $sum $NR | awk '{print $1 / $2}'`
>&2 echo "Average throughput: $avg (xacts/s)" | tee -a aggregate.txt
>&2 echo "Min throughput: $min (xacts/s)" | tee -a aggregate.txt
>&2 echo "Max throughput: $max (xacts/s)" | tee -a aggregate.txt