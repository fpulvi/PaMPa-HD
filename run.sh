#!/bin/bash

cont=0;
cont2=0;
min_sup="15"
num_red="1"
exp_thr="5000"
input="example_1.txt"
output="output_example_1"

hadoop dfs -rm -r $output
hadoop jar target/PampaHD-0.1.0.jar it.polito.dbdmg.pampa_HD.Driver $num_red $input $output $min_sup $exp_thr
rm -r $output
mkdir $output
hadoop dfs -get $output/FI $output