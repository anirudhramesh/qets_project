#!/bin/bash

#SBATCH --time=10:00:00 --mem=4GB --ntasks=1 --cpus-per-task=1 --output='ticker_split.out' --error='ticker_split.err'

python3 ticker\ split\ code\ non\ parallel.py
