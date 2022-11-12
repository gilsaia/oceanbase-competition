#!/bin.bash

# data file
data_file=$HOME/data


case $1 in
    release)
        # obd mirror create -p path
        build_dir=$PWD/build_release
        ;;
    debug)
        # obd mirror create -p path
        build_dir=$PWD/build_debug
        ;;
    *)
        # obd mirror create -p path
        build_dir=$PWD/build_debug
        ;;
esac

cp $build_dir/src/observer/observer $build_dir/usr/local/bin
# yaml file
yaml_file=$PWD/final_2022.yaml

rm -rf $data_file
obd cluster destroy final_2022
obd mirror create -n oceanbase-ce -V 4.0.0.0 -p $build_dir/usr/local/\
 -f -t final_2022
obd cluster autodeploy final_2022 -c $yaml_file

