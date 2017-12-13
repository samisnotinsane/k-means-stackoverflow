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

#function for euclidean distance
def cost_function(x, y):
	return sqrt((x[0]-y[0])**2 + (x[1]-y[1])**2)

#function to assign points to the centroid with minimum distance 
def assign(rdd, centroids):
	d = rdd.map(lambda x: (x, [cost_function(x, centroids[i]) for i in range(k)]))
	return d.map(lambda (x, c): (c.index(min(c)), x))

#calculate new centroid for each cluster(the mean)
def recalculate_centroids(assignments):
	new_centroids = []
	for i in range(k):
		a = assignments.filter(lambda (a, x): a==i).map(lambda (a, x): x)
		a.take(10)
		c = a.sum()/a.count()
		new_centroids.append(c)
	return new_centroids

#function to combine assign and recalculate centroids methods
def iteration(rdd, centroids):
	assigned = assign(rdd, centroids)
	new_centroids = recalculate_centroids(assigned)
	return assigned, new_centroids

#plot the results
centrPlot = plt.figure()
axCenPl = centrPlot.add_subplot(1,1,1)
def draw_clusters(assignments, centroids):
	colours = ['b','g','y']
	for i in range(k):
		all_assigned = np.array(assignments.filter(lambda (a, x): a==i).map(lambda (c,x):x).collect())
		axCenPl.scatter(all_assigned[:,0],all_assigned[:,1],s=1,c=colours[i],marker="o")
		axCenPl.scatter([centroids[i][0]],[centroids[i][1]],s=15,c="r",marker ="o")
	axCenPl.set_ylabel("Number of posts")
	axCenPl.set_xlabel("Reputation")
	axCenPl.set_xscale('log')
	axCenPl.set_yscale('log')
	plt.savefig("plot.png")
	plt.show()

#max iteration
for _ in range(20):
	assigned, new_centroids = iteration(rdd, centroids)
	draw_clusters(assigned, new_centroids)

