#!/usr/bin/env bash

ips=`head -1 config.txt`
ips_arr=(`head -1 config.txt`)

for i in `seq 1 5`; do
    f="conf/node${i}/cassandra.yaml"
    sed -i -e "s/seeds: \".*\"/seeds: \"`echo ${ips// /,}`\"/" ${f}
    listen_address=${ips_arr[$(($i-1))]}
    sed -i -e "s/listen_address:.*/listen_address: ${listen_address}/" ${f}
done