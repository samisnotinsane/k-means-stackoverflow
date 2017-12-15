from __future__ import division
from pyspark import SparkContext, SparkConf, SQLContext
from time import time
from pprint import pprint
from numpy import array
import numpy as np
import os
import matplotlib as mpl
if os.environ.get('DISPLAY','') == '':
    print('no display found. Using non-interactive Agg backend')
    mpl.use('Agg')
import matplotlib
import matplotlib.pyplot as plt
plt.switch_backend('agg')
from math import sqrt

from pyspark.mllib.clustering import KMeans, KMeansModel

conf = SparkConf().setAppName("rddparser")
sc = SparkContext(conf=conf)

users_path = "/data/stackOverflow2017/Users.xml"
#create RDD from Users.xml
usersData = sc.textFile(users_path)
#split line by semicolon
noSemData = usersData.map(lambda line: line.split("\""))
#discard file header and keep Id and Reputation data
idRepData = noSemData.filter(lambda line: len(line)>5).map(lambda line: (line[0],line[1],line[2],line[3]))
#filter out lines without "id" field
idData = idRepData.filter(lambda line : "id" in line[0].lower())
#filter out lines without "reputation" field
reputationData = idData.filter(lambda line : "reputation" in line[2].lower())
#keep Id and Reputation values
idRep = reputationData.map(lambda line: (line[1],line[3]))

posts_path = "/data/stackOverflow2017/Posts.xml"
#create RDD from Posts.xml
postsData = sc.textFile(posts_path)
#split line by "owneruserid"
splitPosts = postsData.map(lambda line: line.lower().split("owneruserid="))
#discard file header and keep OwnerUserId data
ownerIdData = splitPosts.filter(lambda line: len(line)>1).map(lambda line: (line[1]))
#split line by semicolon
ownerIdValues = ownerIdData.map(lambda line: line.split("\""))
#keep OwnerUserId values
ownerId = ownerIdValues.filter(lambda line: len(line)>1).map(lambda line: (line[1]))

#count number of posts for each user
count = ownerId.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)

#join Id/Reputation RDD with Id/number of posts RDD
joinRdd = idRep.map(lambda x:(x[0],x[1])).join(count.map(lambda x:(x[0],x[1])))

#create RDD with values needed for clustering
repNoPosts = joinRdd.map(lambda (k,v): v).cache()

#separating reputation and no of posts in indivudual RDDs
#to be improved/no calling callect()
reputation = repNoPosts.map(lambda (reputation, posts): int(reputation)).collect()
noOfPosts = repNoPosts.map(lambda (reputation, posts): posts).collect()

k = 3
#creating np array from reputation and noOfPosts
X = np.array(list(zip(reputation,noOfPosts))).reshape(len(reputation), 2)

#creating initial centroids, first 3 points in the array
centroids = np.array(X[:k])

#creating and RDD from the np array X
rdd = sc.parallelize(X)
rdd.cache()

# Build the model (cluster the data)

# clusters is a 'KMeansModel' object.
clusters = KMeans.train(rdd, 3, maxIterations=10, initializationMode="random")

def getCentroid(point): 
	return clusters.centers[clusters.predict(point)]

# Evaluate clustering by computing Within Set Sum of Squared Errors
def error(point):
    center = clusters.centers[clusters.predict(point)]
    return sqrt(sum([x**2 for x in (point - center)]))

# Returns a map of unique keys and their occurrence count as values.
centroids = rdd.map( lambda point: getCentroid(point) )
centroidCounts = centroids.map(lambda x: (x, 1)).reduceByKey(lambda (x, y): (x + y))

WSSSE = rdd.map(lambda point: error(point)).reduce(lambda x, y: x + y)
print("Within Set Sum of Squared Error = " + str(WSSSE))

# NumPy array of centroids.
cluster_centres = clusters.centers
# Print location of centroids.
for i in range(len(cluster_centres)):
	print(cluster_centres[i])

rddsample = rdd.take(5)
print(rddsample)

# shows the number of assignments of a given cluster.
print(centroidCounts)
centroidCounts.saveAsTextFile("centroidCounts")