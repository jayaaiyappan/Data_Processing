from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.master("local").appName("SAMPLE_APP").getOrCreate()

firstdf = spark.createDataFrame([{'firstdf-id':1,'firstdf-column1':2,'firstdf-column2':3,'firstdf-column3':4}, \
{'firstdf-id':2,'firstdf-column1':3,'firstdf-column2':4,'firstdf-column3':5}])

firstdf.show()

seconddf = spark.createDataFrame([{'seconddf-id':1,'seconddf-column1':2,'seconddf-column2':4,'seconddf-column3':5}, \
{'seconddf-id':2,'seconddf-column1':6,'seconddf-column2':7,'seconddf-column3':8}])

seconddf.show()

columnsFirstDf = ['firstdf-id', 'firstdf-column1']
columnsSecondDf = ['seconddf-id', 'seconddf-column1']

firstdf.join(seconddf,firstdf['firstdf-id'] == seconddf['seconddf-id']).show()

firstdf.join(seconddf,[col(f) == col(s) for (f, s) in zip(columnsFirstDf, columnsSecondDf)], "inner").show()