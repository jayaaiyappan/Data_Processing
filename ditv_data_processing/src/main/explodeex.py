from pyspark.sql.session import SparkSession
spark = SparkSession.builder.master("local").appName("test").getOrCreate()
df = spark.createDataFrame([(1, "A", [1,2,3]), (2, "B", [3,5])],["col1", "col2", "col3"])
df.show()
from pyspark.sql.functions import explode
df.withColumn("col3", explode(df.col3)).show()