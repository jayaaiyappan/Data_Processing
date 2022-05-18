from pyspark import SparkContext, SparkConf
import configparser
import sys
from datetime import datetime

date_part = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

config = configparser.ConfigParser()
config.read('environment.ini')
env = sys.argv[1]

conf = SparkConf().setMaster(config.get(env, 'executionMode'))\
    .setAppName("Data_Processing")
sc = SparkContext(conf=conf)

orders = sc.textFile(config.get(env, 'base.dir') + "/orders.csv")
orderItems = sc.textFile(config.get(env, 'base.dir') + "/order_item.csv")

ordersFiltered = orders.filter(lambda o: o.split(",")[3] in ("COMPLETE", "CLOSED"))
ordersMap = ordersFiltered.map(lambda o: (int(o.split(",")[0]), o.split(",")[1]))
orderItemsMap = orderItems.map(lambda o: (int(o.split(",")[1]), float(o.split(",")[4])))
ordersJoin = ordersMap.join(orderItemsMap)
ordersJoinMap = ordersJoin.map(lambda o: o[1])
dailyrevenue = ordersJoinMap.reduceByKey(lambda x,y: x + y)
dailyrevenueSorted = dailyrevenue.sortByKey()
dailyrevenueSortedMap = dailyrevenueSorted.map(lambda o: o[0] + "," + str(o[1]))
dailyrevenueSortedMap.saveAsTextFile(config.get(env, 'base.dir') + "/out_dailyrevenue_"+date_part)

#todo https://github.com/dgadiraju/pyspark2-retail/blob/master/src/main/python/TopNDailyProducts.py