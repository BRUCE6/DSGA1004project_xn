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

if __name__ == "__main__":
	spark = SparkSession \
		.builder \
		.appName("compute null value numbers") \
		.getOrCreate()
	df = spark.read.json(sys.argv[1], multiLine = True)
	count_null(df)
