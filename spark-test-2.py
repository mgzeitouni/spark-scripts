data = [1, 2, 3, 4, 5]
distData = sc.parallelize(data)

print(distData.count())