import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import matplotlib.malb as mlab
import matplotlib.pyplot as plt

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
		
def plot_hist(df):
        c_names = df.columns
        height = np.array(df[1])
        bins = np.array(df[0])
        mid_point = bins[:-1]
        widths =[abs(i-j) for i, j in zip(bins[:-1],bins[1:])]
        bar = plt.bar(mid_point,height,width = widths)
        return bar

def show_hist(df):
        c_names = df.columns
        for c in c_names:
                col_hist = df.select(c).flatMap(kambda x:x).histogram(10)
                plot_hist(col_hist)
		
if __name__ == "__main__":
	spark = SparkSession \
		.builder \
		.appName("compute null value numbers") \
		.getOrCreate()
	df = spark.read.json(sys.argv[1], multiLine = True)
	count_null(df)
	count_uniqueness(df)
