from pyspark.sql import SparkSession, Row
#from pyspark.sql.types import *
import datetime
change_capture_schema = Row("Char_name", "Charkey", "Current", "Previous", "Change_type")
#function to get filename as change_capture_weekoftheyear_yyyymmdd.csv
def filename():
	temp = "change_capture_"
	now = datetime.datetime.now()
	weekoftheyear = str(now.isocalendar()[1])
	currentdate = now.strftime("%Y%m%d")
	temp = temp+weekoftheyear+"_"+currentdate+".csv"
	return temp
def change_capture(x,char_name):
	if(char_name =="brand"):
		if x.brand != x.newbrand:
			if x.newbrand:
				change_type ="updated"
			else:	
				change_type ="deleted"
		return change_capture_schema(char_name, x.brand_key, x.brand, x.newbrand,change_type)
	elif(char_name == "Category"):
	
		if x.Category != x.newCategory:
			if x.newCategory:
                        	change_type ="updated"
                	else:   
                        	change_type ="deleted"
                return change_capture_schema(char_name, x.brand_key, x.Category, x.newCategory ,change_type)
	else:
		if x.Vertical != x.newVertical:
			if x.newVertical:
                        	change_type ="updated"
                	else:   
                        	change_type ="deleted"
                return change_capture_schema(char_name, x.brand_key, x.Vertical,x.newVertical,change_type)


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
	hierarchical_data_new = hierarchical_data_new \
				.withColumnRenamed("brand", "newbrand") \
				.withColumnRenamed("Category", "newCategory") \
				.withColumnRenamed("Vertical", "newVertical") 
                                

	brandchanges = hierarchical_data \
		.join(hierarchical_data_new, (hierarchical_data.Product_key == hierarchical_data_new.Product_key) & (hierarchical_data.brand != hierarchical_data_new.newbrand) , 'inner') \
		.select(hierarchical_data.brand_key, hierarchical_data.brand, hierarchical_data_new.newbrand) \
		.distinct()
	# Joining two dataframes to get change in Category
	categorychanges =  hierarchical_data \
               .join(hierarchical_data_new, (hierarchical_data.Product_key == hierarchical_data_new.Product_key) & (hierarchical_data.Category != hierarchical_data_new.newCategory) , 'inner') \
               .select(hierarchical_data.brand_key, hierarchical_data.Category, hierarchical_data_new.newCategory) \
	       .distinct()
	# Joining two dataframs to get change in Vertical
	verticalchanges =  hierarchical_data \
               .join(hierarchical_data_new, (hierarchical_data.Product_key == hierarchical_data_new.Product_key) & (hierarchical_data.Vertical != hierarchical_data_new.newVertical) , 'inner') \
               .select(hierarchical_data.brand_key, hierarchical_data.Vertical, hierarchical_data_new.newVertical) \
	       .distinct()
	
	brandchangesrdd = brandchanges.rdd.map(lambda x: change_capture(x,"brand"))
	categorychangesrdd = categorychanges.rdd.map(lambda x: change_capture(x,"Category"))
	verticalchangesrdd = verticalchanges.rdd.map(lambda x: change_capture(x,"Vertical"))
	tempoutput1 = brandchangesrdd.union(categorychangesrdd)
	output = tempoutput1.union(verticalchangesrdd).toDF()
	temp =filename()
	output.coalesce(1).write.option("header","true").csv("output/"+temp)
