from pyspark.sql import SparkSession

if __name__== "__main__":
	spark = SparkSession \
		.builder \
		.appName("aggregate") \
		.master("local[*]") \
		.getOrCreate()
	
	fakefriends = spark.read \
		      .option("header","true") \
		      .option("inferSchema", value= True) \
		      .csv("/home/maruthimanoj_edu/spark/spark-usecases/example2/data/fakefriends.csv")
	
	fakefriends.printSchema()
	
	avg_friends_by_age = fakefriends.groupBy('age').avg('no_of_friends')
	print(avg_friends_by_age)
	
