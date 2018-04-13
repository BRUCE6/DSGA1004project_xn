import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import numpy as np
from pyspark import SparkContext

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
		
def hist_list(df,ca_pr_type):
        c_names = df.columns
        l = []
        for c in c_names:
                dis = df.select(c).distinct().count()
                count = df.select(c).count()
                print(c)
                if dis < 0.9 * count:
                        try:
                                bins = int(dis*2/np.log10(dis))
                                ran,num = df.select(c).rdd.flatMap(lambda x: x).histogram(bins)
                                print("Numerical")
                                print("Range:{}".format(np.round(ran,2)))
                                print("Count:{}".format(np.round(num,1)))
                        except TypeError:

                                print("Categorical")
                                temp = df.select(c).rdd.flatMap(lambda x: x) \
                                        .map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y)
                                if ca_pr_type == "all":
                                        output = temp
                                else:
                                        #car_pr_type like "top_5","bottom_10",etc.
                                        print(ca_pr_type)
                                        order,num = ca_pr_type.split("_",1)
                                        num = int(num)
                                        if order == "top":
                                                output = sc.parallelize(temp.takeOrdered(num,lambda x: -x[1]))
                                        else:
                                                output = sc.parallelize(temp.takeOrdered(num,lambda x: x[1]))
                                output = output.map(lambda x:"Name:%s  Count:%s" %(x[0],x[1]))
                                print(output.collect())
                else:
                        print("NOT Included")
		
if __name__ == "__main__":
	spark = SparkSession \
		.builder \
		.appName("compute null value numbers") \
		.getOrCreate()
	df = spark.read.json(sys.argv[1], multiLine = True)
	count_null(df)
	count_uniqueness(df)
	hist_list(df,"top_5")

