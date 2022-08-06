from pyspark.mllib.linalg import SparseVector
from pyspark.sql import Row
from pyspark.sql.functions import col

from pyspark.sql.session import SparkSession

spark = SparkSession.builder.master("local").appName("SAMPLE_APP").getOrCreate()

df1 = spark.createDataFrame([
    Row(a=107831, f=SparseVector(
        5, {0: 0.0, 1: 0.0, 2: 0.0, 3: 0.0, 4: 0.0})),
    Row(a=125231, f=SparseVector(
        5, {0: 0.0, 1: 0.0, 2: 0.0047, 3: 0.0, 4: 0.0043})),
])

df1.show()

df2 = spark.createDataFrame([
    Row(a=107831, f=SparseVector(
        5, {0: 0.0, 1: 0.0, 2: 0.0, 3: 0.0, 4: 0.0})),
    Row(a=107831, f=SparseVector(
        5, {0: 0.0, 1: 0.0, 2: 0.0, 3: 0.0, 4: 0.0})),
])

df2.show()

df1.join(df2,df1['a'] == df2['a']).show()

df1.join(df2, ['a']).show()

df1.alias("left").join(df2.alias("right"), ["a"]).select("left.a", "left.f", "right.f").show()
