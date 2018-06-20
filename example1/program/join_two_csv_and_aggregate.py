from pyspark.sql import SparkSession

if __name__ == "__main__":
	session = SparkSession.builder.appName(" join two csv and aggregate").master("local[*]").getOrCreate()
	dataframereader = session.read
        hierarchical_data = dataframereader.option("header","true").option("inferSchema", value = True).csv("/home/maruthimanoj_edu/spark/spark-usecases/Usecase_1/data/sample_data/hierarchical_data.csv")	   
	product_sales_data = dataframereader.option("header","true").option("inferSchema", value = True).csv("/home/maruthimanoj_edu/spark/spark-usecases/Usecase_1/data/sample_data/product_sales_data.csv")
	hierarchical_data.printSchema()
	product_sales_data.printSchema()
	joinandselect = hierarchical_data.join(product_sales_data, hierarchical_data.Product_key == product_sales_data.product_key,'inner').select(hierarchical_data.brand, hierarchical_data.Product_key, product_sales_data.sales,)
	print(joinandselect)
	sumvalue = joinandselect.groupBy('brand').sum('sales')
	print(sumvalue.collect())
	sumvalue.coalesce(1).write.option("header","true").csv("/home/maruthimanoj_edu/spark/spark-usecases/Usecase_1/output/sum")

