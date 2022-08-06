from pyspark.sql import SparkSession
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.functions import sum,col

spark = SparkSession.builder.master("local").appName("sample").getOrCreate()

df_data = spark.read.csv("C:\\Users\\Jaya\\Work\\testdata\\covid_19_india.txt", header=True)
# # df_data.printSchema()
df = df_data\
    .filter("Date >='2021-01-01' and Date <= '2021-04-30'")\
    .groupby(col("State/UnionTerritory"))\
    .agg(sum("Cured").alias("groupsum"))

df.show()
#
# df1 = df_data\
#     .filter(col("State/UnionTerritory") == 'Nagaland')\
#     .filter("Date >='2021-01-01' and Date <= '2021-04-30'")\
#     .agg(sum("Cured").alias("groupsum"))
#
# df1.show()
#
#
# df2 = df_data\
#     .filter((col("State/UnionTerritory") == 'Nagaland') & ((df_data.Date >='2021-01-01') & (df_data.Date <= '2021-04-30')))\
#     .agg(sum("Cured").alias("groupsum"))
#
# df2.show()
#
# df = df_data\
#     .filter("Date >='2021-01-01' and Date <= '2021-04-30'")\
#     .groupby("State/UnionTerritory")\
#     .agg(sum("Cured").alias("groupsum"))\
#     .where(col("State/UnionTerritory") == 'Nagaland')
#
# df.show()

from_date = '2021-01-01'
to_date = '2021-04-30'

# convert string to date time format
import datetime

f_date = datetime.datetime.strptime(from_date,"%Y-%m-%d")
t_date = datetime.datetime.strptime(to_date,"%Y-%m-%d")

# print(f_date)
# 2021-01-01 00:00:00

# print(type(f_date))
# <class 'datetime.datetime'>

# convert datetime to timestamp

# print(datetime.datetime.timestamp(f_date))
# 1609439400.0

ft_date = int(datetime.datetime.timestamp(f_date))
tt_date = int(datetime.datetime.timestamp(t_date))

df_data = df_data.withColumn("unix_ts", unix_timestamp("Date",'yyyy-MM-dd'))
df = df_data\
    .filter((col("unix_ts") >= ft_date) & (col("unix_ts") <= tt_date))\
    .groupby("State/UnionTerritory")\
    .agg(sum("Cured").alias("groupsum"))

df.show()

