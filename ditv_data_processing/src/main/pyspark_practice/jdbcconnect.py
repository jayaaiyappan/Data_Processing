
from pyspark.sql import SparkSession

# intializing the Spark Session
spark=SparkSession.builder\
    .config("spark.jars","C:\\Users\\Jaya\\Work\\downloaded_jars\\mysql-connector-java-8.0.29\\mysql-connector-java-8.0.29.jar")\
    .appName("jdbcconnect").getOrCreate()
# # Loading data from a JDBC source
# jdbcDF = spark.read \
#     .format("jdbc") \
#     .option("url", "jdbc:mysql://localhost:3306/jayawork") \
#     .option("driver", "com.mysql.cj.jdbc.Driver") \
#     .option("dbtable", "item_mast") \
#     .option("user", "root") \
#     .option("password", "root") \
#     .load()

# jdbcDF.show()

# Loading data from a JDBC source
jdbcDF = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", "information_schema.tables") \
    .option("user", "root") \
    .option("password", "root") \
    .load().filter("table_schema = 'jayawork'").select("table_name")

# jdbcDF.show()

tablename_list = [row.table_name for row in jdbcDF.collect()]
# print(table_names_list)

reader = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/jayawork") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("user", "root") \
    .option("password", "root")

for tablename in tablename_list:
    reader.option("dbtable", tablename).load().createTempView(tablename)
    print(tablename)
    spark.sql("select * from " + tablename).show()
# jdbcDF2 = spark.read \
#     .jdbc("jdbc:postgresql:dbserver", "schema.tablename",
#           properties={"user": "username", "password": "password"})
#
# # Specifying dataframe column data types on read
# jdbcDF3 = spark.read \
#     .format("jdbc") \
#     .option("url", "jdbc:postgresql:dbserver") \
#     .option("dbtable", "schema.tablename") \
#     .option("user", "username") \
#     .option("password", "password") \
#     .option("customSchema", "id DECIMAL(38, 0), name STRING") \
#     .load()