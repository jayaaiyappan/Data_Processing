from pyspark.sql.session import SparkSession

spark = SparkSession.builder.master("local").appName("SAMPLE_APP").getOrCreate()

simpleData = [("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100)
  ]
schema = ["employee_name", "department", "salary"]
df = spark.createDataFrame(data=simpleData, schema = schema)
df.printSchema()
df.show(truncate=False)

from pyspark.sql.functions import approx_count_distinct
# df.select(approx_count_distinct("salary")).show()

print(df.select(approx_count_distinct("salary")).collect()[0][0])

from pyspark.sql.functions import countDistinct
df.select(countDistinct("salary")).show()
