#!/bin/bash
#SBATCH -o slurm_%A_%a.out
#SBATCH --array=1-14
#SBATCH --mem-per-cpu=400GB
#SBATCH -p scavenger

module load Python/2.7.11

readarray -t FILES < allyears.txt
FILENAME=${FILES[(($SLURM_ARRAY_TASK_ID - 1))]}
echo $FILENAME
python zippy.py $FILENAME
