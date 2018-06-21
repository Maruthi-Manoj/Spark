from pyspark.sql import SparkSession
import datetime
from datetime import date
#function to get filename as change_capture_weekoftheyear_yyyymmdd.csv
def filename():
        temp = "join_two_csv_and_aggregate_with_dataframes_"
        now = datetime.datetime.now()
        weekoftheyear = str(now.isocalendar()[1])
        currentdate = now.strftime("%Y%m%d")
        temp = temp+weekoftheyear+"_"+currentdate+".csv"
        return temp

if __name__ == "__main__":
	
	spark = SparkSession \
		  .builder \
		  .appName("join two csv and aggregate") \
		  .master("local[*]") \
                  .getOrCreate()
        hierarchical_data = spark.read \
				.option("header","true") \
				.option("inferSchema", value = True) \
				.csv("data/hierarchical_data.csv")	   
	product_sales_data = spark.read \
				.option("header","true") \
				.option("inferSchema", value = True) \
				.csv("data/product_sales_data.csv")
	hierarchical_data.printSchema()
	product_sales_data.printSchema()
	joinandselect = hierarchical_data \
	     .join(product_sales_data, hierarchical_data.Product_key == product_sales_data.product_key,'inner') \
	     .select(hierarchical_data.brand, hierarchical_data.Product_key, product_sales_data.sales)
	print(joinandselect)
	sumvalue = joinandselect.groupBy('brand').sum('sales')
	temp = filename()
	sumvalue \
	.coalesce(1) \
	.write \
	.option("header","true") \
	.csv("output/"+temp)

