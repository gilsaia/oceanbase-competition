#!/bin.bash

# begin server
sh server_run.sh release

# prepare client

# row num
if [ ! "$1" ];then
    row_num=1000000
else
    row_num=$1
fi

# demo file
demo_file=/root/demo.csv
# sample file
sample_file=$HOME/sample.csv

# obclient cmd
ob_cmd="obclient -h 127.0.0.1 -P 2881 -u root -D oceanbase"

# create table cmd
create_table_cmd="create table lineitem_bulk ( \
   l_orderkey BIGINT NOT NULL, \
   I_partkey BIGINT NOT NULL, \
   I_suppkey INTEGER NOT NULL, \
   I_linenumber INTEGER NOT NULL, \
   I_quantity DECIMAL (15, 2) NOT NULL, \
   I_extendedprice DECIMAL (15, 2) NOT NULL, \
   I_discount DECIMAL (15, 2) NOT NULL, \
   I_tax DECIMAL (15, 2) NOT NULL, \
   I_returnflag char (1) DEFAULT NULL, \
   I_linestatus char (1) DEFAULT NULL, \
   I_shipdate date NOT NULL, \
   I_commitdate date DEFAULT NULL, \
   I_receiptdate date DEFAULT NULL, \
   I_shipinstruct char (25) DEFAULT NULL, \
   I_shipmode char (10) DEFAULT NULL, \
   I_comment varchar (44) DEFAULT NULL, \
   primary key (l_orderkey, I_linenumber));"

# get small sample
echo "Begin generate sample"
head -$row_num $demo_file > $sample_file
# exec client cmd
echo "exec client cmd"
echo "set global secure_file_priv=\"\";" | exec $ob_cmd
echo $create_table_cmd | exec $ob_cmd 
echo "set global ob_query_timeout=36000000000;" | exec $ob_cmd

# run perf
echo "run perf"
nohup sudo sh script/utils/perf_run_nostop.sh &

# run load data
echo "load data"
echo "Load data infile \"${sample_file}\" into table lineitem_bulk fields terminated by \"|\";" | exec $ob_cmd

# FlameGraph path
frame_graph_path=$HOME/FlameGraph/

PID=`ps -ef | grep perf | grep -v grep | grep -v perf_test.sh | awk '{print $2}'`
if [ ${#PID} -eq 0 ]
then
    echo "perf not running"
    exit -1
fi
set +e
sudo kill $PID
sudo chmod 777 perf.data
echo "1: perf script"
perf script -i perf.data &> perf.unfold
echo "2: stackcollapse-perf.pl"
${frame_graph_path}stackcollapse-perf.pl perf.unfold &> perf.folded
echo "3: flamegraph.pl"
${frame_graph_path}flamegraph.pl perf.folded > perf.svg
rm -rf perf.data* perf.folded perf.unfold

# drop table
echo "drop table lineitem_bulk;" | exec $ob_cmd

# end server
obd cluster stop final_2022
obd cluster destroy final_2022