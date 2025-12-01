#!/bin/bash

setopt rmstarsilent

./local-submit.sh 2w data-pipeline,feature-extraction,train local-md-gbt-d2_60_7_7
./local-submit.sh 2w data-pipeline,feature-extraction,train local-md-lr-d2_60_7_7
./local-submit.sh 2w data-pipeline,feature-extraction,train local-md-rf-d2_60_7_7
./local-submit.sh 2w data-pipeline,feature-extraction,train local-md-xgb-d2_60_7_7
./work/scripts/visualize_experiments_comparison.py ./work/output/
zip -r local-d2_60_11_X.zip ./work/output/Experience-optimized-local-D2-60-*

rm -rf  ./work/output/Experience-md-gbt-local-D2-60-7-7
rm -rf  ./work/output/Experience-md-lr-local-D2-60-7-7
rm -rf  ./work/output/Experience-md-rf-local-D2-60-7-7
rm -rf  ./work/output/Experience-md-xgb-local-D2-60-7-7
rm -rf  ./work/output/spark-checkpoints/*
rm -rf  ./work/spark-events/*