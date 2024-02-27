#!/bin/bash
#SBATCH --partition=hopper-prod
#SBATCH --job-name=stackv2_download_files
#SBATCH --nodes 1
#SBATCH --ntasks-per-node 1
#SBATCH --cpus-per-task=32
#SBATCH --mem-per-cpu=21G
#SBATCH --output=logs/%x_%j.out
#SBATCH --error=logs/%x_%j.err
#SBATCH --time=7-00:00:00
#SBATCH --array=0-255%32

set -x -e
source ~/.bashrc
source "$CONDA_PREFIX/etc/profile.d/conda.sh"
source activate pyspark
source ../../spark/spark_env.sh

python ~/repos/the-stack-v2/the_stack/2_download_files/get_file_contents.py --blob_prefix ${SLURM_ARRAY_TASK_ID}
