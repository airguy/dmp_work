#!/usr/bin/env python
#-*- coding: utf-8 -*-

import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import Row


if __name__ == "__main__" :

    input_path = sys.argv[1]
    output_path = sys.argv[2]
    job_name = sys.argv[3]

    spark = SparkSession.builder.appName(job_name+' by airguy').getOrCreate()

    daily_df = spark.read.parquet(input_path+"/type=white").select("adid", "language", "package").distinct()
    daily_df.coalesce(40).write.option('compression', 'snappy').mode('overwrite').parquet(output_path)

    sys.exit()
