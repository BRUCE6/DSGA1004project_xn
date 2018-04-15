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

sc = SparkContext()		
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

			
def check_d(ls):
        check_digital = True
        for i in range(len(ls)):
                check_digital = check_digital and ls[i].isdigit()
        return check_digital
def check_c(ls):
        check_char =True
        for i in range(1,len(ls)):
                check_char = check_char and ls[i] == ls[i-1]
        return [check_char,ls[0]]
def check_l(ls):
        check_letter = True
        for i in range(len(ls)):
                check_letter = check_letter and ls[i].isalpha()
        return check_letter
def check_l_u(ls):
        check_letter_U = True
        for i in range(len(ls)):
                check_letter_U = check_letter_U and ls[i].isupper()
        return check_letter_U
def check_l_l(ls):
        check_letter_l = True
        for i in range(len(ls)):
                check_letter_l = check_letter_l and ls[i].islower()
        return check_letter_l
def generate_pattern(ls):
        if check_d(ls):
                return 'd'
        elif check_l(ls):
                if check_l_u(ls):
                        return 'A'
                elif check_l_l(ls):
                        return 'a'
                else:
                        return 'l'
        elif check_c(ls)[0]:
                return check_c(ls)[1]
        else:
                return "_"

def print_pattern(df):
        c_names = df.columns
        for c in c_names:
                index = True
                get_val = df.select(c).rdd.flatMap(lambda x: x).take(100)
                split = []
                pattern = []
                length = len(list(str(get_val[0])))
                for i in range(100):
                        temp = list(str(get_val[i]))
                        if length != len(temp):
                                index = False
                        else:
                                split.append(temp)
                if index:
                        new_df = spark.createDataFrame(split)
                        c_new_names = new_df.columns
                        for c_new in c_new_names:
                                temp = new_df.select(c_new).rdd.flatMap(lambda x: x).collect()
                                pattern.append(generate_pattern(temp))
                        print("{0}: {1}".format(c,''.join(pattern)))

		
import itertools
from datasketch import MinHash
def minhash(df):
	c_names, c_pairs = [], []
	for name, dtype in df.dtypes:
		if dtype == "string":
			c_names.append(name)
	for pair in itertools.combinations(c_names,2):
		c_pairs.append(pair)
	for col1,col2 in c_pairs:
		m1, m2 = MinHash(), MinHash()
		data1 = df.select(col1).rdd.flatMap(lambda x:x).collect()
		data2 = df.select(col2).rdd.flatMap(lambda x:x).collect()
		for d in data1:
			m1.update(d.encode('utf8'))
		for d in data2:
			m2.update(d.encode('utf8'))
		print("Estimated Jaccard for {} and {} is {}".format(col1, col2, m1.jaccard(m2)))
		s1 = set(data1)
		s2 = set(data2)
		actual_jaccard = float(len(s1.intersection(s2)))/float(len(s1.union(s2)))
		print("Actual Jaccard for data1 and data2 is", actual_jaccard)

if __name__ == "__main__":
	spark = SparkSession \
		.builder \
		.appName("compute null value numbers") \
		.getOrCreate()
	df = spark.read.json(sys.argv[1], multiLine = True)
	count_null(df)
	count_uniqueness(df)
	hist_list(df,"top_10")
	print_pattern(df)
