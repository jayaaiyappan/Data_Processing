from pyspark.sql.session import SparkSession
spark = SparkSession.builder.getOrCreate()


df = spark.createDataFrame([["Mobile","Electronics",17000],
                            ["Tubelight","Electronics",2870],
                            ["Wooden Door","Carpentry",6000],
                            ["Window","Carpentry",4300],
                            ["Tap","Plumbing",3000],
                            ["PVC","Plumbing",7000],
                            ["Tubelight","Electronics",2870]],["Product","Type","Revenue"])

# df.show()
# df.repartition("Product").dropDuplicates().explain()


# == Physical Plan ==
# *(2) HashAggregate(keys=[Product#0, Type#1, Revenue#2L], functions=[])
# +- Exchange hashpartitioning(Product#0, Type#1, Revenue#2L, 200), true, [id=#25]
#    +- *(1) HashAggregate(keys=[Product#0, Type#1, Revenue#2L], functions=[])
#       +- *(1) Scan ExistingRDD[Product#0,Type#1,Revenue#2L]
from pyspark.sql.functions import col
df.groupBy(df.columns).count().where(col('count') > 1).show()
df.groupBy(df.columns).count().where(col('count') == 1).show()