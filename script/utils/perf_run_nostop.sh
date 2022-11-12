#!/bin.bash

PID=`ps -ef | grep observer | grep -v grep | awk '{print $2}'`
if [ ${#PID} -eq 0 ]
then
    echo "observer not running"
    exit -1
fi
echo "perf record"
sudo perf record -F 99 -g -p $PID -- sleep 7200