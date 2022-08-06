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
print(df.columns)

print(df.collect())
# print(df.rdd.collect())
# print(type(df.collect()))
# print(type(df.rdd.collect()))
#
# print(df.collect()[0][0])
# print(df.rdd.collect()[0][0])

# to list:
new_list = [row[0] for row in df.collect()]
print(new_list)
print(','.join(str(item) for item in new_list))

# to dict:
new_dict = list(map(lambda row: row.asDict(), df.collect()))[0]
print(new_dict.get("Product"))
print(new_dict.get("Product").encode('ascii','ignore'))
print(new_dict)

