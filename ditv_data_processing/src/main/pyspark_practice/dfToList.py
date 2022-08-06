from pyspark.sql.session import SparkSession
spark = SparkSession.builder.getOrCreate()


df = spark.createDataFrame([["Mobile","Electronics",17000],
                            ["Tubelight","Electronics",2870],
                            ["Wooden Door","Carpentry",6000],
                            ["Window","Carpentry",4300],
                            ["Tap","Plumbing",3000],
                            ["PVC","Plumbing",7000]],["Product","Type","Revenue"])

# df.show()

#to convert to list
mylist = [row[0] for row in df.collect()]
print(mylist)
print(df.select(df['Type']).rdd.map(lambda x:x[0]).collect())
# print(df.select(df['Type']).rdd.flatMap(lambda x:x).collect())
# print(df.select(df['Type']).rdd.map(lambda x:x[0]).collect())
#
# # to convert to list with multiple column
# print(df.select(df['Product'],df['Type']).rdd.collect())
# print(df.select(df['Product'],df['Type']).rdd.flatMap(lambda x:x).collect())

# print(df.select(df['Product'],df['Type']).rdd.map(lambda x:x).collect())
print(df.select(df['Product'],df['Type']).rdd.map(lambda x:x.asDict()).collect())

# print(df.rdd.flatMap(lambda x:x.asDict()).collect())