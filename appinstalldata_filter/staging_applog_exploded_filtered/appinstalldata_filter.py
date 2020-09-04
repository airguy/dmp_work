#!/usr/bin/env python
#-*- coding: utf-8 -*-

import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import Row


def explode_column(row) :
    col_dict = row.asDict()
    adid = col_dict.pop("adid")
    language = col_dict.pop("language")
    app_list = col_dict.pop('pkg_info').split(",")
    for app_info in app_list :
        pkg_name = app_info.split(":")[0]
        if len(pkg_name) > 0 :
            yield Row(adid=adid, language=language, package=pkg_name)




if __name__ == "__main__" :

    work_date = sys.argv[1]
    write_path_prefix = sys.argv[2]
    job_name = sys.argv[3]
    type_mode = sys.argv[4]

    spark = SparkSession.builder.appName(job_name+' by airguy').getOrCreate()

    src_path = {}
    src_path["appguard"] = "/log/appguard/refine"
    src_path["estsecurity"] = "/log/estsecurity/refine"
    src_path["toast_sdk_v3"] = "/log/toast_sdk_v3/refine"
    src_path["onestore"] = "/log/onestore/refine"

    #need to set to white_list
    app_mst_df = spark.read.parquet("/ods/dighty_cdp/master/app/20200829")
    app_mst_df = app_mst_df.select("pkg_name").distinct()

    for src in src_path :
        full_path = src_path[src]+"/"+work_date
            
        print(full_path)
        try :
            app_df = spark.read.parquet(full_path).select("adid", "pkg_info", "language")
            exploded_app_df = app_df.rdd.flatMap(explode_column).toDF()
            filtered_app_df = exploded_app_df.join(app_mst_df, exploded_app_df.package == app_mst_df.pkg_name, how='left')
            filtered_app_df = filtered_app_df.withColumnRenamed("pkg_name", "app_mst_pkg")
            #filtered_app_df = filtered_app_df.withColumn("src", F.lit(src))

            if type_mode in ['ALL', 'white'] :
                white_df = filtered_app_df.filter(F.col("app_mst_pkg").isNotNull()).distinct()
                white_df = white_df.drop("app_mst_pkg")
                white_df.coalesce(20).write.option('compression', 'snappy').mode('append').parquet(write_path_prefix+"/type=white/src="+src)
            
            if type_mode in ['ALL', 'black'] :
                black_df = filtered_app_df.filter(F.col("app_mst_pkg").isNull()).distinct()
                black_df = black_df.drop("app_mst_pkg")
                black_df.coalesce(30).write.option('compression', 'snappy').mode('append').parquet(write_path_prefix+"/type=black/src="+src) 

        except :
            pass 

    sys.exit()
