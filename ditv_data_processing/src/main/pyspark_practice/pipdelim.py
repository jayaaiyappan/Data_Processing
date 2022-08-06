from pyspark.sql import SparkSession

# intializing the Spark Session
spark=SparkSession.builder.appName("example").getOrCreate()

# reading the Dataset as a RDD
datardd=spark.sparkContext.textFile("C:\\Users\\Jaya\\Work\\testdata\\pipdelim.txt")
# datardd.filter(lambda x: x.startswith("1")).map(lambda x: x.split("|")).foreach(print)
datardd.map(lambda x: x.split("|")).foreach(print)
