# https://www.learntospark.com/2020/05/window-function-using-pyspark.html
#Find the top-selling product in each type and order them by the revenue.

from pyspark.sql.session import SparkSession

spark = SparkSession.builder.appName("test").getOrCreate()

df = spark.createDataFrame([["Mobile","Electronics",17000],
                            ["Tubelight","Electronics",2870],
                            ["Wooden Door","Carpentry",6000],
                            ["Window","Carpentry",4300],
                            ["Tap","Plumbing",3000],
                            ["PVC","Plumbing",7000]],["Product","Type","Revenue"])

df.show()

df.createOrReplaceTempView("products")
spark.sql("select Product,Type,Revenue from (select Product,Type,Revenue, rank() over (partition by Type order by revenue desc) as rank from products) t where rank = 1").show()

from pyspark.sql.window import Window
import pyspark.sql.functions as func
window = Window.partitionBy(df['Type']).orderBy(df['Revenue'].desc())
agg_fn = func.rank().over(window)
dfwithrank = df.select(df['Product'], df['Type'], df['Revenue'],agg_fn.alias('rank'))
dfwithrank.show()
dfwithrank.select(dfwithrank['Product'], dfwithrank['Type'], dfwithrank['Revenue']).filter(dfwithrank['rank'] == 1).show()