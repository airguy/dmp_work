#!/bin/sh

START_DATE="20200301"
END_DATE="20200331"

while [ $START_DATE -le $END_DATE ] ;
do
    echo $START_DATE
    ./run.sh

    START_DATE=$(date -d "$START_DATE + 1 day" "+%Y%m%d")
done
