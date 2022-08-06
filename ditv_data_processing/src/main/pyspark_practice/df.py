# # The code below generates two dataframes with the following structure:
# # DF1: uId, uName
# # DF2: uId, pageId, timestamp, eventType.
# # Join the two dataframes using code and count the number of events per uName.
# # It should only output for users who have events in the format uName; totalEventCount.
# def calculate(sparkSession: SparkSession): Unit = {
#     val UIdColName = "uId"
#     val UNameColName = "uName" val CountColName = "totalEventCount" val userRdd: DataFrame = readUserData(sparkSession) val userActivityRdd: DataFrame = readUserActivityData(sparkSession) val res = userRdd .repartition(col(UIdColName)) // ??????????????? . select(col(UNameColName))// ??????????????? result.show() }