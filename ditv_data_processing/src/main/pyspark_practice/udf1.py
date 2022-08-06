# to concatenate from two columns of a dataframe
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local").appName("SAMPLE_APP").getOrCreate()
df = spark.read.csv("C:\\Users\\Jaya\\Work\\testdata\\actor.csv", header="true", inferSchema="true")
df.printSchema()
df.show()

def testFunction(value):
  mystr = value.upper().replace(".", " ").replace(",", " ").replace("  ", " ").strip()
  return mystr

newFunction = F.udf(testFunction, StringType())

# learnt: In PySpark you cannot concatenate StringType columns together using +.
# It will return null which breaks your udf. You can use concat instead.
# df2 = df.withColumn("fullname", newFunction(df.first_name + " " + df.last_name))

df2 = df.withColumn("fullname", newFunction(F.concat(df.first_name, F.lit(" "), df.last_name)))
df2.show()