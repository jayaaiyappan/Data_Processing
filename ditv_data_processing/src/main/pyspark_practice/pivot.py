# https://www.projectpro.io/article/pyspark-interview-questions-and-answers/520
# The pivot() method in PySpark is used to rotate/transpose data from one column into many Dataframe columns
# and back using the unpivot() function ().
# Pivot() is an aggregation in which the values of one of the grouping columns are transposed into separate columns
# containing different data

import pyspark

from pyspark.sql import SparkSession

from pyspark.sql.functions import expr

# Create spark session
spark = SparkSession.builder.master("local").appName("SAMPLE_APP").getOrCreate()

data = [("Banana", 1000, "USA"), ("Carrots", 1500, "USA"), ("Beans", 1600, "USA"),
        ("Orange", 2000, "USA"), ("Orange", 2000, "USA"), ("Banana", 400, "China"),
        ("Carrots", 1200, "China"), ("Beans", 1500, "China"), ("Orange", 4000, "China"),
        ("Banana", 2000, "Canada"), ("Carrots", 2000, "Canada"), ("Beans", 2000, "Mexico")]

columns = ["Product", "Amount", "Country"]

df = spark.createDataFrame(data=data, schema=columns)
#df.printSchema()
df.show(truncate=False)

pivotDF = df.groupBy("Product").pivot("Country").sum("Amount")
#pivotDF.printSchema()
pivotDF.show(truncate=False)
