#!/bin/bash

#SBATCH -t 60:00
#SBATCH --job-name=sorters

#SBATCH --nodes=8
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=2

srun python3 sorter.py