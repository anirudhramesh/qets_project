#!/bin/bash

#SBATCH --time=10:00:00 --mem=30GB --ntasks=2 --cpus-per-task=4 --output='ticker_split_parallel.out' --error='ticker_split_parallel.err'

python3 ticker\ split\ parallel\ pool.py
