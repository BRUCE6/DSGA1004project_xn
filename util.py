import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def count_null(df):
	c_names = df.columns
	list_null = []
	for c in c_names:
		list_null.append(df.filter(col(c).isNull()).count())
	for i in range(len(c_names)):
		print(c_names[i], list_null[i])
		
def count_uniqueness(df):
	c_names = df.columns
	list_uniqueness = []
	for c in c_names:
		list_uniqueness.append(df.select(c).distinct().count())
	for i in range(len(c_names)):
		print(c_names[i], list_uniqueness[i])
		
def hist_list(df):
        c_names = df.columns
        l = []
        for c in c_names:
                dis = df.select(c).distinct().count()
                count = df.select(c).count()
                print(c)
                if dis < 0.6 * count:
                        try:
                                bins = int(0.01*dis)
                                #print("Numerical")
                                temp = df.select(c).rdd.flatMap(lambda x: x).histogram(bins)
                                print("Numerical")
                                print(temp)  
                        except TypeError:
                                print("Categorical")
                                temp = df.select(c).rdd.flatMap(lambda x: x) \
                                        .map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y)
                                print(temp.collect())
		
if __name__ == "__main__":
	spark = SparkSession \
		.builder \
		.appName("compute null value numbers") \
		.getOrCreate()
	df = spark.read.json(sys.argv[1], multiLine = True)
	count_null(df)
	count_uniqueness(df)
	hist_list(df)

