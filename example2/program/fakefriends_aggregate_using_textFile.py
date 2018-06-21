from pyspark import SparkContext, SparkConf
import datetime
from datetime import date
#function to get filename as change_capture_weekoftheyear_yyyymmdd.csv
def filename():
        temp = "fakefriends_aggregate_using_textFile"
        now = datetime.datetime.now()
        weekoftheyear = str(now.isocalendar()[1])
        currentdate = now.strftime("%Y%m%d")
        temp = temp+weekoftheyear+"_"+currentdate+".csv"
        return temp
	
if __name__== "__main__":
	conf = SparkConf() \
		.setAppName("fakefriends aggregate using textFile") \
		.setMaster("local[*]")
	
	sc = SparkContext(conf = conf)
        fakefriends = sc.textFile("data/fakefriends.csv") \
			.map(lambda line: line.split(","))
	fakefriends_without_header = fakefriends.filter(lambda line: line[0]!= 'id')
	age_nooffriends_tuple = fakefriends_without_header \
			.map(lambda line: (int(line[2]),float(line[3])))
	sumoffriends = age_nooffriends_tuple.mapValues(lambda x: (x, 1)) \
			.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
	avg_nooffriends = sumoffriends.mapValues(lambda x: x[0]/x[1])
	temp = filename()
	avg_nooffriends.saveAsTextFile("output/"+temp)
