from pyspark import SparkContext
# Get Item Id and Ratings from u.data file with additional counter value
def getItemIdAndRatings(line):
    temp = line.split("\t")
    return (temp[1],(int(temp[2]),1))
# Get a dictionary with keys as itemIds and values as movie names
def getItemIdAndName():
    with open("u.item",'r') as file:
        lines = file.read().split("\n")
        splitlines = [line.split("|") for line in lines]
        return {split[0] : split[1] for split in splitlines}

if __name__== "__main__":
    sc = SparkContext.getOrCreate()
    sc.setLogLevel('INFO')
    udata = sc.textFile("u.data").map(getItemIdAndRatings)
    # Reduce the ratings based on keys and reduce the counters as well
    sumofratings = udata.reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]))
    #calculate the average of ratings 
    aggregatedratings = sumofratings.mapValues(lambda x: float(x[0])/x[1])
    #broadcast variable with itemIds and Names as a dictionay
    moviemap = sc.broadcast(getItemIdAndName())
    #sort the items in the rdd based on avg ratings
    sortedbyratings = aggregatedratings.map(lambda x: (x[1],x[0])).sortByKey(ascending= False)
    #find the movie name using movie id in the broadcast variable
    finaloutput = sortedbyratings.map(lambda x: (moviemap.value[x[1]], x[0]))
    for x in finaloutput.take(5):
        print(x)
#hello1
