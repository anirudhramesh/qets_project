#!/bin/bash

#SBATCH --time=10:00:00 --mem=20GB --ntasks=1 --cpus-per-task=4 --err='sql_job.err' --out='sql_job.out'

python3 sql_db_maker.py
