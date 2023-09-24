#!/usr/bin/env python
# coding: utf-8

from pyspark.sql import SparkSession
from pyspark.sql import functions as sparkf
from pyspark.sql.functions import when, col, regexp_extract, regexp_replace

def main():
    # สร้าง SparkSession
    spark = (
        SparkSession.builder
            .appName("LoanStats Data Processing")
            .config("spark.executor.memory", "1000m")
            .config("spark.executor.cores", "2")
            .config("spark.cores.max", "6")
            .config("spark.jars.packages", "com.microsoft.azure:spark-mssql-connector:1.0.2")
            .config("spark.sql.debug.maxToStringFields", 200)
            .getOrCreate()
    )

    # อ่านข้อมูลจาก GCS
    loanStat_raw_df = (
        spark.read
            .format("csv")
            .option("header", "true")
            .load("gs://19sep/LoanStats_web.csv")
    )
    
    # ตรวจสอบ schema
    loanStat_raw_df.printSchema()
    
    # ทำการ validation
    valid_df = loanStat_raw_df.withColumn(
        "term_valid", 
        when(col("term").isNull(), "ไม่สามารถประเมินได้")
        .otherwise(
            when(regexp_extract(col("term"), "^(36|60)$", 1) != "", "True")
            .otherwise("False")
        )
    ).withColumn(
        "int_rate_valid", 
        when(col("int_rate").isNull(), "ไม่สามารถประเมินได้")
        .otherwise(
            when(
                (regexp_extract(col("int_rate"), "^([0-9]+[.]?[0-9]*|)$", 1) != "") &
                (regexp_replace(col("int_rate"), "%", "").cast("double") >= 0) & 
                (regexp_replace(col("int_rate"), "%", "").cast("double") <= 30), "True"
            ).otherwise("False")
        )
    ).withColumn(
        "grade_valid", 
        when(col("grade").isNull(), "ไม่สามารถประเมินได้")
        .otherwise(
            when(regexp_extract(col("grade"), "^[A-G]$", 0) != "", "True")
            .otherwise("False")
        )
    )

    # แสดงผล
    valid_df.select("term", "term_valid", "int_rate", "int_rate_valid", "grade", "grade_valid").show()
    
    # แสดงการกระจายข้อมูล
    valid_df.groupBy("term", "term_valid").count().show()
    valid_df.groupBy("int_rate", "int_rate_valid").count().show()
    valid_df.groupBy("grade", "grade_valid").count().show()

    spark.stop()

if __name__ == "__main__":
    main()
