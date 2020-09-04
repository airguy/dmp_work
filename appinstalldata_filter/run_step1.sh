#!/bin/sh

if [ -z $1 ] ; then
    WORK_DATE=`date +"%Y%m%d" -d "1 day ago"`
else
    WORK_DATE=$1
fi

#OUTPUT_PREFIX="/staging/applog/exploded_filtered/dt=$WORK_DATE"
OUTPUT_PREFIX="/staging/applog/history/exploded_filtered/dt=$WORK_DATE"

if [ -z $2 ] ; then
    MODE="ALL"
else
    MODE=$2
fi

JOB_NAME="[step1/2] appinstalldata filter job by airguy"
SCRIPT="appinstalldata_filter.py"

echo "#####################################"
echo $JOB_NAME
echo "work_date: $WORK_DATE"
echo "output: $OUTPUT_PREFIX"
echo "mode: $MODE"
echo "#####################################"

HADOOP=`which hadoop`
if [ "$MODE" == "ALL" ] ; then
    $HADOOP fs -rmr $OUTPUT_PREFIX
else
    $HADOOP fs -rmr $OUTPUT_PREFIX/type=$MODE
fi

SPARK_SUBMIT=`which spark2-submit`

${SPARK_SUBMIT} \
--master yarn \
--deploy-mode client \
--num-executors 30 \
--executor-cores 5 \
--driver-memory 4G --executor-memory 12G \
--py-files ${SCRIPT} \
${SCRIPT} $WORK_DATE $OUTPUT_PREFIX "$JOB_NAME" $MODE
