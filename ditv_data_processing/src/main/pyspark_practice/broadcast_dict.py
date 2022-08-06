from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("sample").getOrCreate()

df_data = spark\
    .read\
    .option("header","true")\
    .option("delimiter","|")\
    .csv("C:\\Users\\Jaya\\Work\\testdata\\us_population.txt")
# df_data.printSchema()
# df_data.show()

lookup = dict({
    "TX": "Texas",
    "NY": "New York",
    "OH": "Ohio",
    "CA": "California",
    "IL": "Illinois",
    "CO": "Colorando",
    "AZ": "Arizona"
})

broad_lookup = spark.sparkContext.broadcast(lookup)

def get_lookup_value(col):
    return broad_lookup.value[col]

from pyspark.sql.functions import udf
myfunc = udf(get_lookup_value)

from pyspark.sql.functions import col
df_data = df_data.withColumn("state", myfunc(col("State_code")))
df_data.show()

# for scala code visit https://www.youtube.com/watch?v=43ahz6Ad_RM&list=PLY6Ag0EOw54wHWNxKE_d5r03LiHXk5coM&index=3