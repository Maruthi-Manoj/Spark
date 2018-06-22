from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
	conf = SparkConf() \
		.setAppName("Customer-orders-total-purchase-by-a-customer") \
		.setMaster("local[*]")
	sc = SparkContext(conf =conf)
	customer_orders_data = sc.textFile('data/customer_orders.csv') \
				 .map(lambda line: line.split(","))
	required_fields = customer_orders_data.map(lambda line: (int(line[0]),float(line[2])))
	totalsum = required_fields.reduceByKey(lambda x, y: x + y)
	
	totalsum.sortByKey(ascending = True) \
			.coalesce(1) \
			.saveAsTextFile("output/customer-orders-total-purchase-by-a-customer")
	
