#!/bin/bash

epoch=$1

finished_dir=$SCRATCHDIR/stokesv_processing/finished/epoch_$1
staging_dir=$SCRATCHDIR/stokesv_processing/staging/epoch_$1

./stage_data.sh $finished_dir $staging_dir
