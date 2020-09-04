#!/bin/sh

if [ -z $1 ] ; then
    WORK_DATE=`date +"%Y%m%d" -d "2 day ago"`
else
    WORK_DATE=$1
fi

INPUT_PATH="/staging/applog/exploded_filtered/dt=$WORK_DATE"
OUTPUT_PATH="/staging/applog/white_merged/dt=$WORK_DATE"

JOB_NAME="[step2/2] merge daily job by airguy"
SCRIPT="merge_daily_appdata.py"

echo "#####################################"
echo $JOB_NAME
echo "work_date: $WORK_DATE"
echo "input: $INPUT_PATH"
echo "output: $OUTPUT_PATH"
echo "#####################################"

HADOOP=`which hadoop`
$HADOOP fs -rmr $OUTPUT_PATH

SPARK_SUBMIT=`which spark2-submit`

${SPARK_SUBMIT} \
--master yarn \
--deploy-mode client \
--num-executors 30 \
--executor-cores 5 \
--driver-memory 4G --executor-memory 8G \
${SCRIPT} $INPUT_PATH $OUTPUT_PATH "$JOB_NAME"
