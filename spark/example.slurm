#!/bin/bash
#SBATCH --partition=production-cluster
#SBATCH --job-name=spark_example
#SBATCH --nodes 2
#SBATCH --ntasks-per-node 1
#SBATCH --cpus-per-task=32
#SBATCH --mem-per-cpu=11G
#SBATCH --output=logs/%x_%j.out
#SBATCH --error=logs/%x_%j.err
#SBATCH --time=4-00:00:00

set -x -e
source ~/.bashrc
source "$CONDA_PREFIX/etc/profile.d/conda.sh"
source activate stack_v2
source spark_env.sh

spark-start

spark-submit --master $SPARK_URL app.py

spark-stop