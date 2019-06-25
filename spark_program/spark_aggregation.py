#Aggregations:
#Taking a group of element and perform some operations in it which return one value.
#example the addition of n numbers is aggregation.
#similarly we can get min and max of those integers

#example file is:
#fdf,2,rww,dfdff,99.77
#fdf,3,rww,dfdff,87654.9
#fdf,2,rww,dfdff,229.9
#fdf,2,rww,dfdff,235.9

orderitems = sc.textFile("/FileStore/tables/Sacramentorealestatetransactions.csv")

#we can count a data by
orderitems.count()

for i in orderitems.take(10):
	print(i)

#to filter
orderfilter = orderitems.filter(lambda oi : int(oi.split(" , ")[1]) == 2)

for i in orderfilter.take(10):
	print(i)

orderitemsubtotal = orderfilter.map(lambda oi : float(oi.split(" , ")[4]))

for i in orderitemsubtotal.take(10):
	print(i)

#by this we get a numbers of 4th column
#now a task is to add those numbers
#Ex :
#99.77
#229.9
#235.9


from operator import add

#it reduces and add
orderitemsubtotal.reduce(add)

orderitemsubtotal.reduce(lambda x, y: x + y)

_________________________


#flatMap
a = sc.textFile("/FileStore/tables/Sacramentorealestatetransactions.csv")

c = a.                                                                                                                                                                  flatMap(lambda oi : oi.split(",")[2])
#this will return a RDD
_________________________

#filter
a = sc.textFie("/FileStore/tables/Sacramentorealestatetransactions.csv")

d = a.filter(lambda oi = oi.split(",")[4] == 22)

print i in d.take(10):
	print(i)

d = a.filter(lam)


#conditions through lambda in spark
orderitemfilter = d.reduce(add)

orderitemfilter = d.reduce(lambda x,y: x + y)

orderitemfilter = d.reduce(lambda x,y: x if(float(x.split(",")[4]) < float(y.split(",")[4])) else y )


#Get count by status
orderstatus = orders.map(lambda o: (o.split(",")[3],1))

for i in orderstatus.take(10):
	print(i)

countbystatus = orderstatus.counByKey()

countbystatus



#joins
#join two variables by .join or .leftouterjoin to join two different variables
#same concept as sql






