from pyspark.sql import SparkSession
import datetime
#function to get filename as change_capture_weekoftheyear_yyyymmdd.csv
def filename():
        temp = "fakefriends_aggregate_using_tempview"
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
        fakefriends.createOrReplaceTempView("fakefriends")
	avg_friends_by_age = spark.sql("SELECT age, avg(no_of_friends) FROM fakefriends GROUP BY age ORDER BY age ASC")		
	avg_friends_by_age.coalesce(1).write.option("header","true").csv("output/"+temp)
	
