#!/bin/bash

#SBATCH --time=10:00:00 --mem=15GB --ntasks=1 --cpus-per-task=4

python3 ticker\ split\ code.py
