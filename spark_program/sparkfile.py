#work on databrics cluster
#databrics will give a cluster of 6GB to work on it
#start with spark
#download any data in csv format from google

import pyspark

filee = sc.textFile("/FileStore/tables/Sacramentorealestatetransactions.csv"")

#it will give first line from csv file as a output
filee.first()

#display 10 output from csv file
for i in filee.take(10):
	print(i)
________________________


#make a RDD(Resilient distributed dataset) form

#if a file is not in RDD form(if it is a list or tupple or a dict form) than convert it into rdd form

l = range(1,10)
print(l)

Out[26]: range

lRDD = sc.parallelize(l)
print(lRDD)

output:- PythonRDD[243] at RDD at PythonRDD.scala:56
__________________________


lRDD.count()

Out[30]: 9


