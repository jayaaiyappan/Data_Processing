# from pyspark.sql.session import SparkSession
# from pyspark.sql.functions import explode,col
#
# spark = SparkSession.builder.master("local").appName("test").getOrCreate()
#
# # data = '{"userId": 1, "someString": "example1", "varA": [0, 2, 5], "varB": [1, 2, 9]}', '{"userId": 2, "someString": "example2", "varA": [1, 20, 5], "varB": [9, null, 6]}'
# data = '{"userId": 1, "someString": "example1","varA": [0, 2, 5], "varB": [1, 2, 9]}'
# df = spark.read.json(spark.sparkContext.parallelize[data])
#
# df.show()
#
# todo
# https://stackoverflow.com/questions/33220916/explode-transpose-multiple-columns-in-spark-sql-table