# https://www.learntospark.com/2021/11/add-leading-zeros-to-columns-in-a-spark-dataframe-using-pyspark.html
from pyspark.sql.session import SparkSession

spark = SparkSession.builder.appName('Sample').getOrCreate()

# creating datafrome from list
list_data=[["Babu",20],["Raja",8],["Mani",75],["Kalam",100],["Zoin",7],["Kal",53]]
df = spark.createDataFrame(list_data,["name","score"])
df.show()

# format_string
from pyspark.sql.functions import format_string
df1 = df.withColumn("score_000",format_string('%03d','score'))
df1.show()

# lpad
from pyspark.sql.functions import lpad
df2 = df.withColumn("score_000",lpad('score',3,'0'))
df2.show()

# concat & substring
from pyspark.sql.functions import concat,substring,lit
df3=df.withColumn("score_000",concat(lit("00"),"score"))
df3.show()
df4=df3.withColumn("score_000",substring("score_000",-3,3))
df4.show()