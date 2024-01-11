#!/bin/bash

# GLOBAL settings
SPARK_HOME=${SPARK_HOME:-"/fsx/bigcode/spark/spark-3.5.0-bin-hadoop3"}
SCRATCH=${SCRATCH:-"/scratch/$USER"}
SPARK_MASTER_PORT=${SPARK_MASTER_PORT:-7077}
SPARK_MASTER_WEBUI_PORT=${SPARK_MASTER_WEBUI_PORT:-8080}

# resources for spark's accounting processes on each node
SPARK_DAEMON_CORES=1
SPARK_DAEMON_MEMORY=1024
# resources for the application's driver process (pyspark's main()) on the master node
SPARK_DRIVER_CORES=2
SPARK_DRIVER_MEMORY=8096

PYSPARK_PYTHON=${PYSPARK_PYTHON:-$(which python)}
PYSPARK_DRIVER_PYTHON=${PYSPARK_DRIVER_PYTHON:-$(which python)}


function spark-start() {
    # Verify if we are in a SLURM allocation or not
    if [[ -z "${SLURM_JOB_NAME}" || -z "${SLURM_CPUS_PER_TASK}" || -z "${SLURM_MEM_PER_CPU}" || -z "${SLURM_JOB_NUM_NODES}" ]]; then
        echo "Error: Some required SLURM environment variables are missing."
        echo "This script should only be run within a SLURM job."
        echo "SLURM_JOB_NAME: ${SLURM_JOB_NAME}"
        echo "SLURM_JOB_NUM_NODES: ${SLURM_JOB_NUM_NODES}"
        echo "SLURM_CPUS_PER_TASK: ${SLURM_CPUS_PER_TASK}"
        echo "SLURM_MEM_PER_CPU: ${SLURM_MEM_PER_CPU}"
        exit 1
    fi

    # Access to spark-submit
    export PATH="$SPARK_HOME/bin:$PATH"

    # Initialize spark WORKER, CONF, LOG and TMP dirs
    SPARK_WORK_DIR=${SCRATCH}/spark/${SLURM_JOB_NAME##*/}_${SLURM_JOB_ID}
    SPARK_WORKER_DIR=${SPARK_WORK_DIR}
    SPARK_CONF_DIR=${SPARK_WORK_DIR}/conf
    SPARK_LOG_DIR=${SPARK_WORK_DIR}/log
    SPARK_LOCAL_DIRS=${SPARK_WORK_DIR}/tmp

    srun -l mkdir -p "${SPARK_WORK_DIR}" "${SPARK_CONF_DIR}" "${SPARK_LOG_DIR}" "${SPARK_LOCAL_DIRS}" \
        && srun -l chmod -R 766 "${SPARK_WORK_DIR}"

    SPARK_MASTER_HOST=$(scontrol show hostname ${SLURM_NODELIST} | head -n 1)
    export SPARK_URL="spark://${SPARK_MASTER_HOST}:${SPARK_MASTER_PORT}"

    # The driver runs only on the master node
    MASTER_COMPUTE_CORES=$((SLURM_CPUS_PER_TASK - SPARK_DAEMON_CORES - SPARK_DRIVER_CORES))
    MASTER_COMPUTE_MEMORY=$((SLURM_MEM_PER_CPU * SLURM_CPUS_PER_TASK - SPARK_DAEMON_MEMORY - SPARK_DRIVER_MEMORY))
    # The resources available on the rest of the nodes
    WORKER_COMPUTE_CORES=$((SLURM_CPUS_PER_TASK - SPARK_DAEMON_CORES))
    WORKER_COMPUTE_MEMORY=$((SLURM_MEM_PER_CPU * SLURM_CPUS_PER_TASK - SPARK_DAEMON_MEMORY))

    export SPARK_DEFAULTS="${SPARK_CONF_DIR}/spark-defaults.conf"
    cat << EOF > "${SPARK_DEFAULTS}.tmp"
spark.master                    ${SPARK_URL}
spark.submit.deployMode         client
spark.ui.showConsoleProgress    false
spark.ui.enabled                true
spark.jars.packages             org.apache.hadoop:hadoop-aws:3.3.4,org.apache.spark:spark-hadoop-cloud_2.12:3.3.4

spark.sql.sources.commitProtocolClass                      org.apache.spark.internal.io.cloud.PathOutputCommitProtocol
spark.sql.parquet.output.committer.class                   org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter
spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a  org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory
spark.hadoop.fs.s3a.committer.name           magic
spark.hadoop.fs.s3a.committer.magic.enabled  true
spark.hadoop.fs.s3a.committer.threads        ${SLURM_CPUS_PER_TASK}
spark.hadoop.fs.s3a.buffer.dir               ${SPARK_LOCAL_DIRS}/s3a

spark.local.dir                              ${SPARK_LOCAL_DIRS}
spark.sql.warehouse.dir                      ${SPARK_LOCAL_DIRS}/warehouse
spark.sql.autoBroadcastJoinThreshold         -1

spark.driver.maxResultSize                   8192m
spark.driver.memory                          ${SPARK_DRIVER_MEMORY}m
spark.executor.memory                        ${MASTER_COMPUTE_MEMORY}m
spark.network.timeout                        1200
EOF
    sbcast "${SPARK_DEFAULTS}.tmp" "${SPARK_DEFAULTS}"

    export SPARK_LAUNCHER=${SPARK_WORK_DIR}/spark-launcher.sh
    cat << EOF > "${SPARK_LAUNCHER}.tmp"
#!/bin/bash
export SPARK_HOME=${SPARK_HOME}
export SPARK_WORKER_DIR=${SPARK_WORKER_DIR}
export SPARK_LOG_DIR=${SPARK_LOG_DIR}
export SPARK_LOCAL_DIRS=${SPARK_LOCAL_DIRS}
export SPARK_CONF_DIR=${SPARK_CONF_DIR}

export SPARK_MASTER_HOST=${SPARK_MASTER_HOST}

export SPARK_DAEMON_CORES=${SPARK_DAEMON_CORES}
export SPARK_DAEMON_MEMORY=${SPARK_DAEMON_MEMORY}m
export SPARK_DRIVER_CORES=${SPARK_DRIVER_CORES}
export SPARK_DRIVER_MEMORY=${SPARK_DRIVER_MEMORY}m

export PYSPARK_PYTHON=${PYSPARK_PYTHON}
export PYSPARK_DRIVER_PYTHON=${PYSPARK_DRIVER_PYTHON}

source "$SPARK_HOME/sbin/spark-config.sh"
source "$SPARK_HOME/bin/load-spark-env.sh"

if [[ \${SLURM_PROCID} -eq 0 ]]; then
    # Start a master + worker on the same node
    export SPARK_WORKER_CORES=${MASTER_COMPUTE_CORES}
    export SPARK_WORKER_MEMORY=${MASTER_COMPUTE_MEMORY}m

    "${SPARK_HOME}/bin/spark-class" org.apache.spark.deploy.master.Master &> "${SPARK_LOG_DIR}/spark-master.log" &
    MASTER_PID=$!
    exec "${SPARK_HOME}/bin/spark-class" org.apache.spark.deploy.worker.Worker ${SPARK_URL} &> "${SPARK_LOG_DIR}/spark-worker.log" &
    WORKER_PID=$!
    wait $MASTER_PID $WORKER_PID
else
    # Start a worker
    export SPARK_WORKER_CORES=${WORKER_COMPUTE_CORES}
    export SPARK_WORKER_MEMORY=${WORKER_COMPUTE_MEMORY}m

    "${SPARK_HOME}/bin/spark-class" org.apache.spark.deploy.worker.Worker ${SPARK_URL}  &> "${SPARK_LOG_DIR}/spark-worker.log" &
    WORKER_PID=$!
    wait $WORKER_PID
fi
EOF
    chmod +x "${SPARK_LAUNCHER}.tmp"
    sbcast "${SPARK_LAUNCHER}.tmp" "${SPARK_LAUNCHER}"

    srun --label --export=ALL --wait=0 "${SPARK_LAUNCHER}" &

    max_attempts=20
    attempt=0
    while ! grep -q "started at http://" "${SPARK_LOG_DIR}/spark-master.log"; do
        if (( attempt++ == max_attempts )); then
            echo "Error: Connection to Spark master not established after $(( max_attempts * 5 )) seconds."
            exit 1
        fi
        sleep 5
    done
}


function spark-stop() {
    # todo: check for running spark processes
    srun sh -c "rm -rf ${SPARK_WORK_DIR}"
}