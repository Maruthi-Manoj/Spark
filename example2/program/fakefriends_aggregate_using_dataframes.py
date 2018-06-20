from pyspark.sql import SparkSession
import datetime
from datetime import date
#function to get filename as change_capture_weekoftheyear_yyyymmdd.csv
def filename():
        temp = "groupby_avg_age_"
        now = datetime.datetime.now()
        weekoftheyear = str(now.isocalendar()[1])
        currentdate = now.strftime("%Y%m%d")
        temp = temp+weekoftheyear+"_"+currentdate+".csv"
        return temp

if __name__== "__main__":
	spark = SparkSession \
		.builder \
		.appName("aggregate") \
		.master("local[*]") \
		.getOrCreate()
	
	fakefriends = spark.read \
		      .option("header","true") \
		      .option("inferSchema", value= True) \
		      .csv("data/fakefriends.csv")
	
	fakefriends.printSchema()
	temp = filename()
	avg_friends_by_age = fakefriends.groupBy('age').avg('no_of_friends')
	avg_friends_by_age.coalesce(1).write.option("header","true").csv("output/"+temp)
	