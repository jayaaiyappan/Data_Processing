from pyspark.sql import SparkSession
from pyspark.sql import Row
appName= "hive_pyspark"
master= "local"

spark = SparkSession.builder.master(master).appName(appName).enableHiveSupport().getOrCreate()
df_data = spark.read.csv("path",header=True)
df_data.write.saveAsTable("table_name")
spark.sql("select * from table_name limit 10").show()
