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
                if dis < 0.6 * count:
			bins = int(dis/10.0)
                        print(c)
                        try:
                                print(df.select(c).rdd.flatMap(lambda x: x).histogram(bins))
                        except TypeError:
                                continue

		
if __name__ == "__main__":
	spark = SparkSession \
		.builder \
		.appName("compute null value numbers") \
		.getOrCreate()
	df = spark.read.json(sys.argv[1], multiLine = True)
	count_null(df)
	count_uniqueness(df)
	hist_list(df)

