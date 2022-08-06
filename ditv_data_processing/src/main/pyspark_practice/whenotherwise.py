from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("sample").getOrCreate()

df_data = spark\
    .read\
    .option("header","true")\
    .csv("C:\\Users\\Jaya\\Work\\testdata\\txn.txt")

df_data.show()

from pyspark.sql.functions import col
from pyspark.sql.functions import when
from pyspark.sql.functions import sum


df_data_amt = df_data.withColumn("amt",
                             when(col("Transaction type")=='debit',
                                  -1*col("Amount"))
                             .otherwise(col("Amount")))
df_data_amt.show()

df_data_final = df_data_amt.groupby(col("Customer_No")).agg(sum(col("amt")))
df_data_final.show()

# df_data.groupby(col("Customer_No")).pivot(col("Transaction Type")).agg(sum(col("Amount"))).show()

df = df_data.groupby("Customer_No").pivot("Transaction Type").agg(sum("Amount"))
df = df.withColumn("total", col("credit") - col("debit"))
df.show()