# https://discuss.itversity.com/t/processing-json-data-using-spark-2-python-and-scala/19138
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import explode,col

spark = SparkSession.builder.master("local").appName("test").getOrCreate()

# PS C:\users\Jaya> curl https://data.gharchive.org/2015-01-01-15.json.gz -OutFile "C:\users\Jaya\20150101.json.gz"
data = spark.read.json("C:\\Users\\Jaya\\Work\\testdata\\file5.json")
data.printSchema()
data.show(truncate=False)
# print(data.count())

data_new = data.withColumn("rest_exploded", explode(col("restaurants")))
data_new = data_new.drop(col("restaurants"))
# print(data_new.count())
data_new.show(truncate=False)
data_new.printSchema()
data_new = data_new.select("results_found", "status", "code", "results_shown", "message", "results_start","rest_exploded.restaurant.*")
data_new.show(truncate=False)

# res_data = data.select("restaurants")
# res_data.printSchema()
# res_data.show()
# res_data1 = data.select(explode(col("restaurants")))
# res_data1.printSchema()
# res_data1.show(truncate=False)
#
# res_data1.select("col.restaurant.*").show()


