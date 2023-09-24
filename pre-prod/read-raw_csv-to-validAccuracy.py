#!/usr/bin/env python
# coding: utf-8

# In[1]:


#! apt-get update -y


# In[2]:


#! apt-get install wget -y


# In[3]:


#! apt-get install unzip -y


# In[4]:


#! wget LoanStats_web.csv.zips://storage.googleapis.com/19sep/LoanStats_web.csv.zip


# In[5]:


#! unzip LoanStats_web.csv.zip


# In[6]:


from pyspark.sql import functions as sparkf


# In[7]:


from pyspark.sql import SparkSession

# สร้าง SparkSession ใหม่
spark = (
    SparkSession.builder
        # กำหนดชื่อแอปพลิเคชัน
        .appName("Integration of onlinePassenger and Station")
        # กำหนด URL ของ Spark master
        .master("spark://spark-master:7077")
        # กำหนดจำนวน memory ที่ executor จะใช้
        .config("spark.executor.memory", "1000m")
        # กำหนดจำนวน cores ที่ executor จะใช้
        .config("spark.executor.cores", "2")
        # กำหนดจำนวน cores สูงสุดที่ Spark จะใช้
        .config("spark.cores.max", "6")
        # สร้าง SparkSession ถ้ายังไม่มี, ถ้ามีแล้วจะใช้ SparkSession ที่มีอยู่
        .config("spark.jars.packages", "com.microsoft.azure:spark-mssql-connector:1.0.2") \
        .config("spark.sql.debug.maxToStringFields", 200) \

        .getOrCreate()
)


# In[8]:


# ใช้ SparkSession ที่เราสร้างขึ้น (spark) ในการอ่านข้อมูล

loanStat_raw_df = (
    spark.read  # ใช้วิธีการอ่านข้อมูล (read method)
        .format("csv")  # กำหนดรูปแบบของไฟล์ที่จะอ่านเป็น CSV
        .option("header", "true")  # กำหนดว่าไฟล์ CSV มี header ที่บรรทัดแรก
        .load("LoanStats_web.csv")  # โหลดข้อมูลจากไฟล์
)


# In[9]:


loanStat_raw_df.printSchema()

#กำหนด columns ที่ธุรกิจให้คำแนะนำฯ ไว้

businessAttrs_df = ["loan_amnt","term","int_rate"\
                                ,"installment","grade","emp_length",\
                           "home_ownership","annual_inc"\
                                ,"verification_status","loan_status",\
                           "purpose","addr_state","dti","delinq_2yrs"\
                                ,"earliest_cr_line",\
                           "open_acc","pub_rec"\
                                ,"revol_bal","revol_util","total_acc","issue_d"]loanStat_raw_df.select(businessAttrs_df).describe().toPandas().transpose()
# In[10]:


from pyspark.sql.functions import when, col, regexp_extract, regexp_replace

# กำหนดรูปแบบ regex สำหรับการตรวจสอบ
term_pattern = "^(36|60)$"
int_rate_pattern = "^([0-9]+[.]?[0-9]*|)$"  # ตัวเลขทั่วไป หรือ ตัวเลขที่มีจุดทศนิยม
grade_pattern = "^[A-G]$"

# ทำการ validation
valid_df = loanStat_raw_df.withColumn(
    "term_valid", 
    when(col("term").isNull(), "ไม่สามารถประเมินได้")
    .otherwise(
        when(regexp_extract(col("term"), term_pattern, 1) != "", "True")
        .otherwise("False")
    )
).withColumn(
    "int_rate_valid", 
    when(col("int_rate").isNull(), "ไม่สามารถประเมินได้")
    .otherwise(
        when(
            (regexp_extract(col("int_rate"), int_rate_pattern, 1) != "") &
            (regexp_replace(col("int_rate"), "%", "").cast("double") >= 0) & 
            (regexp_replace(col("int_rate"), "%", "").cast("double") <= 30), "True"
        ).otherwise("False")
    )
).withColumn(
    "grade_valid", 
    when(col("grade").isNull(), "ไม่สามารถประเมินได้")
    .otherwise(
        when(regexp_extract(col("grade"), grade_pattern, 0) != "", "True")
        .otherwise("False")
    )
)

# แสดงผล
valid_df.select("term", "term_valid", "int_rate", "int_rate_valid", "grade", "grade_valid").show()


# In[11]:


valid_df.groupBy("term", "term_valid").count().show()


# In[12]:


valid_df.groupBy("int_rate", "int_rate_valid").count().show()


# In[13]:


valid_df.groupBy("grade", "grade_valid").count().show()


# In[15]:


valid_df.filter(col('int_rate_valid')== "ไม่สามารถประเมินได้").toPandas().transpose()


# In[ ]:




