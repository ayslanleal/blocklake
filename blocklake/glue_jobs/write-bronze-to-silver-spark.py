import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

df = spark.read.csv(
    "s3://block-lakehouse/blocks/*",
    sep ="\t",
    header=True 
)

df = df.withColumn(
    "date_partition", 
    to_date(df.time)
)

df.write \
    .partitionBy('date_partition') \
    .mode('overwrite') \
    .format('parquet') \
    .save("s3://block-lakehouse/silver/bitcoin/")
df_deffilama = spark.read.json(
    "s3://block-lakehouse/Deffilama/tvl/*.json"
).withColumn("protocol", input_file_name())

df_deffilama = df_deffilama.withColumn(
    "protocol",
    regexp_replace("protocol","s3://block-lakehouse/Deffilama/tvl/","")
).withColumn(
    "protocol",
    regexp_replace("protocol",".json","")
).withColumn(
    "date",
    from_unixtime(col("date"),"yyyy-MM-dd")
)

df_deffilama.write \
    .partitionBy('date') \
    .mode('overwrite') \
    .format('parquet') \
    .save("s3://block-lakehouse/silver/deffillama/")

job.commit()