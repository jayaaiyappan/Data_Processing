from pyspark.sql.session import SparkSession

spark = SparkSession.builder.getOrCreate()

df1 = spark.createDataFrame([[1,'jaya'],[2,'natu']],['id','name'])
df2 = spark.createDataFrame([[1,'39'],[2,'40'],[1,'39']],['id','age'])

df1.join(df2, df1.id == df2.id).show()
# below avoids  the join column appearing twice
df1.join(df2, "id").show()

