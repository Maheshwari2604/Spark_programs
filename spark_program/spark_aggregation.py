#Aggregations:
#Taking a group of element and perform some operations in it which return one value.
#example the addition of n numbers is aggregation.
#similarly we can get min and max of those integers



orderitems = sc.textFile("/FileStore/tables/Sacramentorealestatetransactions.csv")

for i in orderitems.take(10):
	print(i)


orderfilter = orderitems.filter(lambda oi = int(oi.split(" , ")[1]) == 2)

for i in orderfilter.take(10):
	print(i)
