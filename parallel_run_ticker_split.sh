#!/bin/bash

#SBATCH --time=10:00:00 --mem=20GB --ntasks=1 --cpus-per-task=4 --output='ticker_split_parallel.out' --error='ticker_split_parallel.err'

python3 ticker\ split\ code.py
