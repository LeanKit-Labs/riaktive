#!/bin/bash

# ulimit 
ulimit -n 65536

# Add dirs
for i in log yz yz_anti_entropy leveldb anti_entropy ring; do
    if [ ! -d "/data/riak/${i}" ];then
        mkdir -p "/data/riak/${i}"
    fi
done
chown -R riak:riak /data 

riak console

