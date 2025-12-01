#!/bin/bash

setopt rmstarsilent

./local-submit.sh 2w data-pipeline,feature-extraction,train local-optimized-d2_60_1_1
./local-submit.sh 2w data-pipeline,feature-extraction,train local-optimized-d2_60_3_3
./local-submit.sh 2w data-pipeline,feature-extraction,train local-optimized-d2_60_5_5
./local-submit.sh 2w data-pipeline,feature-extraction,train local-optimized-d2_60_7_7
./local-submit.sh 2w data-pipeline,feature-extraction,train local-optimized-d2_60_9_9
./local-submit.sh 2w data-pipeline,feature-extraction,train local-optimized-d2_60_11_11
./work/scripts/visualize_experiments_comparison.py ./work/output/
zip -r local-d2_60_11_X.zip ./work/output/Experience-optimized-local-D2-60-*

rm -rf  ./work/output/Experience-optimized-local-D2-60-1-1
rm -rf  ./work/output/Experience-optimized-local-D2-60-3-3
rm -rf  ./work/output/Experience-optimized-local-D2-60-5-5
rm -rf  ./work/output/Experience-optimized-local-D2-60-7-7
rm -rf  ./work/output/Experience-optimized-local-D2-60-9-9
rm -rf  ./work/output/Experience-optimized-local-D2-60-11-11
rm -rf  ./work/output/spark-checkpoints/*
rm -rf  ./work/spark-events/*
