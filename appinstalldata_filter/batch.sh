#!/bin/sh

START_DATE="2020-08-28"
END_DATE="2020-08-30"

DATE_LIST=`php -f /home1/irteam/work/airguy/utils/date_list.php $START_DATE $END_DATE`

for WORK_DATE in $DATE_LIST
do
    echo "$WORK_DATE"
    ./run_step1.sh $WORK_DATE
    #./run_step2.sh $WORK_DATE
done
