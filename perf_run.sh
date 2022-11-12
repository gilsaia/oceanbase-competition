#!/bin.bash

# FlameGraph path
frame_graph_path=$HOME/FlameGraph/

PID=`ps -ef | grep observer | grep -v grep | awk '{print $2}'`
if [ ${#PID} -eq 0 ]
then
    echo "observer not running"
    exit -1
fi
echo "1: perf record"
perf record -F 99 -g -p $PID -- sleep 60
echo "2: perf script"
perf script -i perf.data &> perf.unfold
echo "3: stackcollapse-perf.pl"
${frame_graph_path}stackcollapse-perf.pl perf.unfold &> perf.folded
echo "4: flamegraph.pl"
${frame_graph_path}flamegraph.pl perf.folded > perf.svg
rm -rf perf.data* perf.folded perf.unfold