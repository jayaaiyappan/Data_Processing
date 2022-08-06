from pyspark.sql.session import SparkSession
from pyspark.sql.functions import expr

if __name__ == '__main__':

  spark = SparkSession.builder.master("local").appName("SAMPLE_APP").getOrCreate()
  df = spark.createDataFrame([(1,[0.2, 2.1, 3., 4., 3., 0.5]),(2,[7., 0.3, 0.3, 8., 2.,])],['id','column'])
  df.show()
  df.printSchema()

  #https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.functions.aggregate.html
  df.withColumn("column<2", expr("aggregate(filter(column, x -> x < 2), 0D, (x, acc) -> acc + x)")) \
    .withColumn("column>2", expr("aggregate(filter(column, x -> x > 2), 0D, (x, acc) -> acc + x)")) \
    .withColumn("column=2", expr("aggregate(filter(column, x -> x == 2), 0D, (x, acc) -> acc + x)")) \
    .show(truncate=False)