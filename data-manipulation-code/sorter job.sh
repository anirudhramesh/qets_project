#!/bin/bash

#SBATCH --time=10:00:00 --ntasks=1 --cpus-per-task=4 --mem=15GB

python3 magic_cleaner.py 'taq_sep_2019_quotes' 'taq_sep_2019_trades'
