#!/bin/bash

setopt rmstarsilent

./local-submit.sh 4w data-pipeline,feature-extraction,train local-md-lr-d2_60_7_7
./local-submit.sh 4w data-pipeline,feature-extraction,train local-md-rf-d2_60_7_7
./local-submit.sh 4w data-pipeline,feature-extraction,train local-md-xgb-d2_60_7_7
./work/scripts/visualize_experiments_comparison.py ./work/output/
