#!/bin.bash

# row num
row_num=$1

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
head -$row_num $demo_file > $sample_file
# exec client cmd
echo "set global secure_file_priv=\"\";" | exec $ob_cmd
echo $create_table_cmd | exec $ob_cmd 
echo "set global ob_query_timeout=36000000000;" | exec $ob_cmd
echo "Load data infile \"${sample_file}\" into table lineitem_bulk fields terminated by \"|\";" | exec $ob_cmd
echo "drop table lineitem_bulk;" | exec $ob_cmd