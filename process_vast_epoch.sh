#!/bin/bash

epoch=$1
in_dir=$SCRATCHDIR/data/vast/epoch_$1
out_dir=$SCRATCHDIR/stokesv_processing/finished/epoch_$1

echo python run_selavy.py $in_dir --out-dir=$out_dir
