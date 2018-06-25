from pyspark.sql import SparkSession
import datetime
#function to get filename as join_two_csv_and_sum_using_tempview__weekoftheyear_yyyymmdd.csv
def filename():
        temp = "join_two_csv_and_sum_using_tempview_"
        now = datetime.datetime.now()
        weekoftheyear = str(now.isocalendar()[1])
        currentdate = now.strftime("%Y%m%d")
        temp = temp+weekoftheyear+"_"+currentdate+".csv"
        return temp

if __name__ == "__main__":
	
	spark = SparkSession \
		  .builder \
		  .appName("join two csv and sum using tempview") \
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
	hierarchical_data.createOrReplaceTempView("hierarchical_data")
	product_sales_data.createOrReplaceTempView("product_sales_data")
	sumvalue = spark.sql("SELECT brand, sum(sales) FROM hierarchical_data, product_sales_data WHERE hierarchical_data.Product_key == product_sales_data.product_key GROUP BY brand")
	temp = filename()
	sumvalue \
	.coalesce(1) \
	.write \
	.option("header","true") \
	.csv("output/"+temp)

