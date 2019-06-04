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
ordreitems.count()

for i in orderitems.take(10):
	print(i)

#to filter
orderfilter = orderitems.filter(lambda oi = int(oi.split(" , ")[1]) == 2)

for i in orderfilter.take(10):
	print(i)

orderitemsubtotal = orderfilter.map(lambda oi = float(oi.split(" , ")[4]))

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
