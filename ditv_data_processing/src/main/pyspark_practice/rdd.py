# https://sairamdgr8.medium.com/acing-apache-spark-rdd-interview-questions-series-1-using-pyspark-96eb0b8aeb41

## importing Modules

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# intializing the Spark Session
spark=SparkSession.builder.appName("Read Write RDD").getOrCreate()

# reading the Dataset as a RDD
datardd=spark.sparkContext.textFile("C:\\Users\\Jaya\\Work\\testdata\\property.txt")

# print(type(datardd))
# datardd.foreach(print)

datardd_columns_new=datardd.filter(lambda x: x.startswith("Property_ID"))
# print(datardd_columns_new.collect())

## reading data lines
datardd_data_rows=datardd.filter(lambda x: not x.startswith("Property_ID"))
# print(datardd_data_rows.collect()) # returns array
datardd_data_rows.foreach(print)
list = datardd_data_rows.collect()
print(list)
# datardd_data_rows_map=datardd_data_rows.map(lambda x:x.split('|'))
# datardd_data_rows_map.foreach(print)
# print(type(datardd_data_rows_map))
# list = datardd_data_rows_map.collect()
# print(list)
# print(type(list))
# print(datardd_data_rows_collect.take(4))

### getting the index of respective columns to do tranforming new colum

datardd=spark.read\
    .option("header","true")\
    .option("inferSchema","true")\
    .option("delimiter", '|')\
    .csv("C:\\Users\\Jaya\\Work\\testdata\\property.txt")
print(datardd.collect())