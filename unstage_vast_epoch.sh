#!/bin/bash

epoch=$1

finished_dir=$SCRATCHDIR/stokesv_processing/finished/epoch_$1
staging_dir=$SCRATCHDIR/stokesv_processing/staging/epoch_$1

mv $staging_dir/* $finished_dir
