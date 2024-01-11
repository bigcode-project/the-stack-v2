#!/bin/bash
#SBATCH --partition=hopper-prod
#SBATCH --job-name=stackv2_file_paths
#SBATCH --nodes 8
#SBATCH --ntasks-per-node 1
#SBATCH --cpus-per-task=32
#SBATCH --mem-per-cpu=21G
#SBATCH --output=logs/%x_%j.out
#SBATCH --error=logs/%x_%j.err
#SBATCH --time=4-00:00:00

set -x -e
source ~/.bashrc
source "$CONDA_PREFIX/etc/profile.d/conda.sh"
source activate pyspark
source ../../spark/spark_env.sh

spark-start

spark-submit --properties-file "$SPARK_DEFAULTS" find_file_paths.py

spark-stop