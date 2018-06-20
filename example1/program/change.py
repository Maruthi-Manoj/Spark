from pyspark.sql import SparkSession
import datetime
from datetime import date
#function to get filename as change_capture_weekoftheyear_yyyymmdd.csv
def filename():
	temp = "change_capture_"
	now = datetime.datetime.now()
	weekoftheyear = str(now.isocalendar()[1])
	currentdate = now.strftime("%Y%m%d")
	temp = temp+weekoftheyear+"_"+currentdate+".csv"
	return temp

if __name__ == "__main__":
	
	spark = SparkSession \
		.builder \
		.appName("change") \
		.master("local[*]") \
		.getOrCreate()
	
	#Reading data from csv as a dataframe
        
	hierarchical_data = spark.read \
				.option("header","true") \
				.option("inferSchema", value = True) \
				.csv("data/hierarchical_data.csv")	   
	hierarchical_data_new = spark.read \
					.option("header","true") \
					.option("inferSchema", value = True) \
					.csv("data/hierarchical_data_new.csv")
	
	# Joining two dataframes  to get change in brandnames	
	brandchanges = hierarchical_data \
		.join(hierarchical_data_new, (hierarchical_data.Product_key == hierarchical_data_new.Product_key) & (hierarchical_data.brand != hierarchical_data_new.brand) , 'inner') \
		.select(hierarchical_data.brand_key, hierarchical_data.brand, hierarchical_data_new.brand) \
		.distinct()
	# Joining two dataframes to get change in Category
	categorychanges =  hierarchical_data \
               .join(hierarchical_data_new, (hierarchical_data.Product_key == hierarchical_data_new.Product_key) & (hierarchical_data.Category != hierarchical_data_new.Category) , 'inner') \
               .select(hierarchical_data.brand_key, hierarchical_data.Category, hierarchical_data_new.Category) \
	       .distinct()
	# Joining two dataframs to get change in Vertical
	verticalchanges =  hierarchical_data \
               .join(hierarchical_data_new, (hierarchical_data.Product_key == hierarchical_data_new.Product_key) & (hierarchical_data.Vertical != hierarchical_data_new.Vertical) , 'inner') \
               .select(hierarchical_data.brand_key, hierarchical_data.Vertical, hierarchical_data_new.Vertical) \
	       .distinct()
	a=brandchanges.collect()
	b=categorychanges.collect()
	c=verticalchanges.collect()
	print(a)
	print(b)
	print(c)
	#sumvalue.coalesce(1).write.option("header","true").csv("/home/maruthimanoj_edu/spark/spark-usecases/Usecase_1/output/sum")
