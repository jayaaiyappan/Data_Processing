# https://discuss.itversity.com/t/processing-json-data-using-spark-2-python-and-scala/19138
from pyspark.sql.session import SparkSession

spark = SparkSession.builder.master("local").appName("test").getOrCreate()

# PS C:\users\Jaya> curl https://data.gharchive.org/2015-01-01-15.json.gz -OutFile "C:\users\Jaya\20150101.json.gz"
data = spark.read.json("C:\\Users\\Jaya\\Work\\testdata\\20150101.json.gz")
data.printSchema()
# data.show()

# data.filter("payload.comment.id IS NOT NULL").select("payload.comment.*", "repo.*").show()

from pyspark.sql.functions import *
finalDF = data.filter("payload.issue.id IS NOT NULL"). \
  select("repo.id", "repo.name", to_date("payload.issue.created_at").alias("created_at")). \
  groupBy("id", "name", "created_at"). \
  agg(count(lit(1)).alias("issue_count"))

print(finalDF.count())


