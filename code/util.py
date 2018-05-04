import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pandas as pd
import numpy as np
from pyspark import SparkContext
from pyspark.mllib.clustering import KMeans, KMeansModel
import itertools
from datasketch import MinHash
import matplotlib.mlab as mlab
import matplotlib.pyplot as plt
from nltk import ngrams
import timeit

sc = SparkContext()
def count_null(df):
	c_names = df.columns
	list_null = []
	for c in c_names:
		list_null.append(df.filter(col(c).isNull()).count())
	#for i in range(len(c_names)):
	#	print(c_names[i], list_null[i])
	return list_null
	
def count_distinct(df):
	c_names = df.columns
	list_distinct = []
	for c in c_names:
		list_distinct.append(df.select(c).distinct().count())
	#for i in range(len(c_names)):
	#	print(c_names[i], list_distinct[i])
	return list_distinct


def check_d(ls):
	if sum([ls[i].isdigit() for i in range(len(ls))]) != len(ls): 
		return False
	else:
		return True
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
	if sum([ls[i].isdigit() for i in range(len(ls))]) == len(ls):
		return 'd'
	elif sum([ls[i].isalpha() for i in range(len(ls))]) == len(ls):
		if sum([ls[i].isupper() for i in range(len(ls))]) == len(ls):
			return 'A'
		elif sum([ls[i].islower() for i in range(len(ls))]) == len(ls):
			return 'a'
		else:
			return 'l'
	elif ls.count(ls[0]) == len(ls):
		return ls[0]
	else: 
		return "_ "

def print_pattern(df):
	c_names = df.columns
	for c in c_names:
		index = True
		count = df.select(c).count()
		if count > 100:
			count = int(np.log2(count))*10
		get_val = df.select(c).rdd.flatMap(lambda x: x).takeSample(False,count)
		split = []
		pattern = []
		for i in range(count):
			temp = list(str(get_val[i]))
			if len(list(str(get_val[0]))) != len(temp):
				index = False
			else:
				split.append(temp)
		if index:
			new_df = spark.createDataFrame(split)
			c_new_names = new_df.columns
			for c_new in c_new_names:
				temp = new_df.select(c_new).rdd.flatMap(lambda x: x).collect()
				pattern.append(generate_pattern(temp))
			if pattern.count("_ ") > 5:
				print("{0}: {1} (number of _ :{2})".format(c,''.join(pattern),pattern.count("_ ")))
			else:
				print("{0}: {1}".format(c,''.join(pattern)))

def single_minhash(df):
	c_names = []
	for name, dtype in df.dtypes:
		if dtype == "string":
			c_names.append(name)
	for col1,col2 in itertools.combinations(c_names,2):
		m1, m2 = MinHash(), MinHash()
		data1 = df.select(col1).rdd.flatMap(lambda x:x).collect()
		data2 = df.select(col2).rdd.flatMap(lambda x:x).collect()
		for d in data1:
			for i in ngrams(d,4):
				m1.update(''.join(i).encode('utf-8'))
		for d in data2:
			for i in ngrams(d,4):
				m2.update(''.join(i).encode('utf-8'))
		print("MinHash Similarity for {} and {} is {}".format(col1, col2, m1.jaccard(m2)))
		#s1 = set(data1)
		#s2 = set(data2)
		#actual_jaccard = float(len(s1.intersection(s2)))/float(len(s1.union(s2)))
		#print("Actual Jaccard for data1 and data2 is", actual_jaccard)

def multi_minhash(df1, df2):
	c_names1, c_names2 = []
	for name, dtype in df1.dtypes:
		if dtype == "string":
			c_names1.append(name)
	for name, dtype in df2.dtypes:
		if dtype == "string":
			c_names2.append(name)
	for col1,col2 in itertools.product(c_names1, c_names2):
		m1, m2 = MinHash(), MinHash()
		data1 = df1.select(col1).rdd.flatMap(lambda x:x).collect()
		data2 = df2.select(col2).rdd.flatMap(lambda x:x).collect()
		for d in data1:
			for i in ngrams(d,4):
				m1.update(''.join(i).encode('utf-8'))
		for d in data2:
			for i in ngrams(d,4):
				m2.update(''.join(i).encode('utf-8'))
		print("MinHash Similarity for {} and {} is {}".format(col1, col2, m1.jaccard(m2)))
		
def candidate_key(df, num, list_uniqueness):
	#num = df.count()
	#list_distinct = count_distinct(df)
	#list_uniqueness = [n == num for n in list_distinct]
	#print(list_uniqueness)
	c_names = df.columns
	list_dict = []
	list_dict.append({c_names[i]:list_uniqueness[i] for i in range(len(c_names))})
	# only candidate key less than size 5 are considered
	for i in range(min(4, len(c_names))):
		if i == 0:
			continue
		# false meaning not a candidate key
		list_dict.append({k:False for k in itertools.combinations(c_names, i+1)})
	#print(list_dict)
	for i in range(len(list_dict)):
		if i== 0:
			continue
		for key in list_dict[i].keys():
			flag_sub = 0 # 1 means subset is a unique
			list_cmb = itertools.combinations(key, i)
			for cmb in list_cmb:
				# combinations 1 returns (some,)
				if i == 1:
					cmb = cmb[0]
				#if cmb not in list_dict[i - 1].keys() or list_dict[i - 1][cmb] == True:
				# 'superkey' meaning superkey not candidate
				if list_dict[i - 1][cmb] == True or list_dict[i-1][cmb] == 'superkey':
					#list_dict[i].pop(key) # cannot change dictionary size during iteration
					list_dict[i][key] = 'superkey'
					flag_sub = 1
					break
			if not flag_sub:
				tmp_num = df.select(*key).distinct().count()
				if tmp_num == num:
					list_dict[i][key] = True
	#print(list_dict)
	list_candidate = []
	for d in list_dict:
		for key in d.keys():
			if d[key] == True:
				list_candidate.append(key)
	#print(list_candidate)
	return list_candidate
###cluster to generate patterns
def error(point):
	center = clusters.centers[clusters.predict(point)]
	return sqrt(sum([x**2 for x in (point - center)]))



def num_hist(ls):
	num_record = []
	for c in ls:
		dis = df.select(c).distinct().count()
		count = df.select(c).count()
		print("Column Name: {}".format(c))
		if dis == 2 and count != 2:
			print("Type: Binary")
			binary = df.select(c).rdd.flatMap(lambda x:x) \
					.map(lambda x: (str(x),1)).reduceByKey(lambda x,y:x+y) \
					.map(lambda x:"Name:%s  Count:%s" %(x[0],x[1]))
			print(binary.collect())
		elif dis < count:
			bins = int(np.log2(dis))
			ran,num = df.select(c).rdd.flatMap(lambda x: x).histogram(bins)
			print("Type: Numerical")
			num_record.append(c)
			a = []
			for i in range(1,len(ran)):
				a.append("Range:(%s,%s) Count: %s" % (np.round(ran[i-1],2),np.round(ran[i],2),np.round(num[i-1],1)))
			print(a)
		else:
			print("Type: Numerical")
			print("Information Not Included")
	return(num_record)

def cate_count(ls,ca_pr_type):
	for c in ls:
		dis = df.select(c).distinct().count()
		count = df.select(c).count()
		if dis < count:
			temp = df.select(c).rdd.flatMap(lambda x: x) \
				.map(lambda x:(str(x),1)).reduceByKey(lambda x,y:x+y)
			print("Column Name: {}".format(c))
			#check the type
			t = temp
			ls_p = t.map(lambda x:x[0]).take(10)
			split = []
			pattern = []
			index = False
			for i in range(10):
				t1 = list(str(ls_p[i]))
				if len(list(str(ls_p[0]))) == len(t1):
					split.append(t1)
					index = True
			if index:	
				new_df = spark.createDataFrame(split)
				c_new_names = new_df.columns
				for c_new in c_new_names:
					temp1 = new_df.select(c_new).rdd.flatMap(lambda x: x).collect()
					pattern.append(generate_pattern(temp1))
				if sum([i == 'd' for i in pattern[0:4]]) == 4 and sum([i == 'd' for i in pattern[5:7]]) == 2:
					Type = "Type: Date"
				else:
					Type = "Type: Categorical"
			print(Type)
			#print result
			if ca_pr_type == "all":
				output = temp
			else:
				print("%s group:" % ca_pr_type)
				order,num = ca_pr_type.split("_",1)
				num = int(num)
				if order == "top":
					output = sc.parallelize(temp.takeOrdered(num,lambda x: -x[1]))
				else:
					output = sc.parallelize(temp.takeOrdered(num,lambda x: x[1]))
			output = output.map(lambda x:"Name:%s  Count:%s" %(x[0],x[1]))
			print(output.collect())
		else:
			print("Type: Text")
			print("Detalied Information Not Included")


def print_hist(df,ca_pr_type):
	#ca_pr_type can be "top_5" or "Bottom_5",etc
	num_ls  = []
	str_ls = []
	for i in df.dtypes:
		if list(i)[1] == "string":
			str_ls.append(list(i)[0])
		else:
			num_ls.append(list(i)[0])	
	num_rec_ls = num_hist(num_ls)
	cate_count(str_ls,ca_pr_type)
	return(num_rec_ls)	

if  __name__ == "__main__":
	spark = SparkSession \
		.builder \
		.appName("Data Profiler") \
		.getOrCreate()
	df = spark.read.json(sys.argv[1], multiLine = True)
	print('*'*50)
	print(sys.argv[1])
	num = df.count()
	print('{0} rows'.format(num))

	print('*'*50)
	print("Null value number:")
	start = timeit.default_timer()
	list_null = count_null(df)
	stop = timeit.default_timer()
	for i in range(len(list_null)):
		print("{0:15s}:{1}".format(df.columns[i], list_null[i]))
	print('[{0:.2f}s]'.format(stop - start))

	print('*'*50)
	print("Column uniqueness:")
	start = timeit.default_timer()
	list_distinct = count_distinct(df)
	list_uniqueness = [n == num for n in list_distinct]
	stop = timeit.default_timer()
	for i in range(len(list_uniqueness)):
		print("{0:15s}:{1}".format(df.columns[i], list_uniqueness[i]))
	print('[{0:.2f}s]'.format(stop - start))

	print('*'*50)
	print("Candidate keys:")
	start = timeit.default_timer()
	list_candidate = candidate_key(df, num, list_uniqueness)
	stop = timeit.default_timer()
	for c in list_candidate:
		print(c)
	print('[{0:.2f}s]'.format(stop - start))
	
	print('*'*50)
	#print_pattern(df.na.drop(how="all"))
	#single_minhash(df)
	#multi_minhash(df1,df2)
	#print_hist(df,"top_10")
