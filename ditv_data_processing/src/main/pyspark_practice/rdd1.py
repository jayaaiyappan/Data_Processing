# https://www.learntospark.com/2021/12/spark-interview-questions-and-answer-coding-round.html
from pyspark.sql.session import SparkSession

# intializing the Spark Session
spark = SparkSession.builder.appName("sample").getOrCreate()
rdd_in = spark.sparkContext.textFile("C:\\Users\\Jaya\\Work\\testdata\\real_estate.txt")
print(rdd_in.first())

#header
header=rdd_in.filter(lambda l: l.startswith("Property_ID"))
# get index of the column
col_list=header.first().split('|')
f1=col_list.index("Property_ID")
f2=col_list.index("Location")
f3=col_list.index("Size")
f4=col_list.index("Price_SQ_FT")

# create new header based on the requirement
header_out=header.map(lambda x: x.split("|")[f1]+"|"+x.split("|")[f2]+"|Final_Price")
print(header_out.first())

data_rdd = rdd_in.filter(lambda row: not row.startswith("Property"))
print(data_rdd.first())

rdd2=data_rdd.map(lambda x:x.split('|'))
print(rdd2.first())


# Calculate Final_Price: Final_Price = (Size * Price_SQ_FT)
def mul_price(d1,d2):
    return str(float(d1) * float(d2))


rdd3=rdd2.map(lambda x: x[f1]+"|"+x[f2]+"|"+ mul_price(x[f3],x[f4]))
rdd3.foreach(print)

final_out=header_out.union(rdd3)

#Save the final Spark RDD as textfile
final_out.coalesce(1).saveAsTextFile("output.txt")
