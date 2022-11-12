#!/bin.bash

# data file
data_file=$HOME/data
# obd mirror create -p path
mirror_create_path=$PWD/build_release/usr/local/

# yaml file
yaml_file=$PWD/final_2022.yaml

rm -rf $data_file
obd cluster destroy final_2022
obd mirror create -n oceanbase-ce -V 4.0.0.0 -p $mirror_create_path\
 -f -t final_2022
obd cluster autodeploy final_2022 -c $yaml_file

