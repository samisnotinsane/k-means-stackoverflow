from time import time
from pprint import pprint
from numpy import array
from math import sqrt
import numpy as np
import matplotlib.pyplot as plt

from pyspark import SparkContext, SparkConf, SQLContext
# from pyspark.mllib.clustering import KMeans, KMeansModel

def cost_function(x, y):
    return sqrt( (int(x[0])-int(y[0]))**2 + (int(x[1])-int(y[1]))**2 )

conf = SparkConf().setAppName("rddparser")
sc = SparkContext(conf=conf)

# /Users/sameenislam/Documents/Big_Data/cw2/sample_data/posts_sample.xml
# /Users/sameenislam/Documents/Big_Data/cw2/sample_data/user_sample.xml
# /data/stackOverflow2017/Users.xml
# /data/stackOverflow2017/Posts.xml

users_path = "/data/stackOverflow2017/Users.xml"
print("Parsing user data...")
#create RDD from Users.xml
usersData = sc.textFile(users_path)
#split line by quote marks
noSemData = usersData.map(lambda line: line.split("\""))
#discard file header and keep Id and Reputation data
idRepData = noSemData.filter(lambda line: len(line)>5).map(lambda line: (line[0],line[1],line[2],line[3]))
#only keep lines with "id" field
idData = idRepData.filter(lambda line : "id" in line[0].lower())
#only keep lines with "reputation" field
reputationData = idData.filter(lambda line : "reputation" in line[2].lower())
#extract Id and Reputation values
idRep = reputationData.map(lambda line: (line[1],line[3]))
print("User data parsing complete")

posts_path = "/data/stackOverflow2017/Posts.xml"
print("Parsing post data...")
#create RDD from Posts.xml
postsData = sc.textFile(posts_path)
#split line by "owneruserid"
splitPosts = postsData.map(lambda line: line.lower().split("owneruserid="))
#discard file header and only keep OwnerUserId value
ownerIdData = splitPosts.filter(lambda line: len(line)>1).map(lambda line: (line[1]))
#split line by quote marks
ownerIdValues = ownerIdData.map(lambda line: line.split("\""))
#keep OwnerUserId values
ownerId = ownerIdValues.filter(lambda line: len(line)>1).map(lambda line: (line[1]))
#count number of posts for each user
count = ownerId.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
print("Post data parsing complete")

# Hint: stored in format (Id, (Rep, noOfPost)).
# E.g. (u'772521', (u'1538', 94)).
joinRdd = idRep.map( lambda x: (x[0], x[1]) ).join(count.map( lambda x: (x[0], x[1]) ))

# returns list of inner tuples consisting of (reputation, number of posts)
# E.g. [(u'1538', 94), (u'101', 12), ...].
repCount = joinRdd.values().cache()
repCountParsed = repCount.map(lambda (r, p): int(r))
# print(repCount.take(4))

K = 4
# train k-means algorithm
print("Training K-Means model with K=" + str(K) + "...")
# data = repCount.map(parseVector).cache()
# print(data.take(4))
centroids = repCount.takeSample(False, K, 1)
# compute list of distances, d, to all the centroids
# e.g. d: [((u'1', 1), [10.04987562112089, 5.0, 53.85164807134504, 20.0])]
d = repCount.map(lambda x: (x, [cost_function(x, centroids[i]) for i in range(K)]))
# pick smallest distance to centroid from d
# c is an array with distances to K centroids; uses min to pick smallest distance to centroid
# e.g. [(2, (u'164', 8)), (1, (u'1', 1)), (2, (u'6', 1))
assignments = d.map( lambda (x, c):  (c.index(min(c)), x ))
# blue, green, yellow, magenta
colours = ['b', 'g', 'y', 'm']
for i in range(K):
    all_assigned = np.array(assignments.filter(lambda (a, x): a==i).map(lambda (c, x): x))
    plt.scatter(all_assigned[:,0], all_assigned[:,1], color=colours[i])
    plt.scatter([centroids[i][0]], [centroids[i][1]], color['r'] )
plt.show()
plt.gcf().clear()
print("K-Means model single iteration complete")
# print(assignments.take(10))


# epsilon = 0
# prototypes = data.takeSample(False, K, 1)
# norm = 1


# centroid, centroid_history, point_owners = train(ds=repCount, k=4)
# print("Centroids: " + centroid)
# Graph.plot(repCount, centroid_history, point_owners)

# sampleList = repNoPosts.takeSample(False, 250)
# sampleRdd = sc.parallelize(sampleList)
# sampleRdd.saveAsTextFile("so2017/takesample/cogrouped")

# for e in sampleRdd.collect():
#     print(e)

# print(combData.collect())

# Code below is yet to be audited.
#join Id/Reputation RDD with Id/number of posts RDD
# joinRdd = idRep.map(lambda x:(x[0],x[1])).join(count.map(lambda x:(x[0],x[1])))

#create RDD with values needed for clustering
# repNoPosts = joinRdd.map(lambda (k,v): v)

# print(joinRdd.take(10))
