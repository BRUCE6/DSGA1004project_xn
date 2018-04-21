import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import numpy as np
from pyspark import SparkContext
import itertools
from datasketch import MinHash

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

sc = SparkContext()		
def hist_list(df,ca_pr_type):
    c_names = df.columns
        l = []
        for c in c_names:
            dis = df.select(c).distinct().count()
            count = df.select(c).count()
            print("Column Name: {}".format(c))
            if dis < 0.9 * count:
                try:
                    bins = int(dis*2/np.log10(dis))
                    ran,num = df.select(c).rdd.flatMap(lambda x: x).histogram(bins)
                    print("Type: Numerical")
                    a = []
                        for i in range(1,len(ran)):
                            a.append("Range:(%s,%s) Count: %s" % (np.round(ran[i-1],2),np.round(ran[i],2),np.round(num[i-1],1)))
                                print(a)
                    except TypeError:
                        temp = df.select(c).rdd.flatMap(lambda x: x) \
                            .map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y)
                                t = temp
                                ls_p = t.map(lambda x:x[0]).take(10)
                                split = []
                                pattern = []
                                for i in range(10):
                                    t1 = list(str(ls_p[i]))
                                    split.append(t1)
                                new_df = spark.createDataFrame(split)
                                c_new_names = new_df.columns
                                for c_new in c_new_names:
                                    temp1 = new_df.select(c_new).rdd.flatMap(lambda x: x).collect()
                                    pattern.append(generate_pattern(temp1))
                                
                                if sum([i == 'd' for i in pattern[0:4]]) == 4 and sum([i == 'd' for i in pattern[5:7]]) == 2:
                                    print ("Type: Date")
                                else:
                                    print("Type: Categorical")

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
                print("Distribution / Groups NOT Included")

			
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
                        count = int(np.log2(count)) * 10
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

def candidate_key(df):
	num = df.count()
	list_distinct = count_distinct(df)
	list_uniqueness = [n == num for n in list_distinct]
	#print(list_uniqueness)
	c_names = df.columns
	list_dict = []
	list_dict.append({c_names[i]:list_uniqueness[i] for i in range(len(c_names))})
	for i in range(len(c_names)):
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
	
if __name__ == "__main__":
	spark = SparkSession \
		.builder \
		.appName("compute null value numbers") \
		.getOrCreate()
	df = spark.read.json(sys.argv[1], multiLine = True)
	#print(num)
	#list_null = count_null(df)
	#list_distinct = count_distinct(df)
	#print(list_distinct)
	#hist_list(df,"top_10")
	print_pattern(df.na.drop(how="all"))
	print(candidate_key(df))

