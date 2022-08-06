from pyspark.sql.session import SparkSession
spark = SparkSession.builder.master("local").appName("SAMPLE_APP").getOrCreate()

df = spark.sql("SELECT 1 AS id, array(NAMED_STRUCT('name', 'frank','age', 40,'state', 'Texas'), "
               "NAMED_STRUCT('name', 'maria','age', 51,'state', 'Georgia')) AS array_of_structs")

df.show(truncate=False)
df.printSchema()
df.createTempView("sample")

df1 = spark.sql("SELECT  explode(array_of_structs) "
                "FROM sample")
df1.show()

df2 = spark.sql("SELECT  id, person.name, person.age "
                "FROM sample "
                "LATERAL VIEW explode(array_of_structs) exploded_people as person")
df2.show()

df2 = spark.sql("SELECT  id, name, age "
                "FROM sample "
                "LATERAL VIEW inline(array_of_structs) exploded_people as name,age,state")
df2.show()

