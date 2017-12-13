from time import time
from pprint import pprint
from numpy import array
from math import sqrt

# from cluster import KMeans, Graph

from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.mllib.clustering import KMeans, KMeansModel

conf = SparkConf().setAppName("rddparser")
sc = SparkContext(conf=conf)

# /Users/sameenislam/Documents/Big_Data/cw2/sample_data/posts_sample.xml
# /Users/sameenislam/Documents/Big_Data/cw2/sample_data/user_sample.xml
# /data/stackOverflow2017/Users.xml
# /data/stackOverflow2017/Posts.xml

users_path = "/Users/sameenislam/Documents/Big_Data/cw2/sample_data/user_sample.xml"
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
#print(idRep.take(10))

posts_path = "/Users/sameenislam/Documents/Big_Data/cw2/sample_data/posts_sample.xml"
#create RDD from Posts.xml
postsData = sc.textFile(posts_path)
#split line by "owneruserid"
splitPosts = postsData.map(lambda line: line.lower().split("owneruserid="))
#discard file header and only keep OwnerUserId value
ownerIdData = splitPosts.filter(lambda line: len(line)>1).map(lambda line: (line[1]))
#split line by semicolon
ownerIdValues = ownerIdData.map(lambda line: line.split("\""))
#keep OwnerUserId values 
ownerId = ownerIdValues.filter(lambda line: len(line)>1).map(lambda line: (line[1]))
#count number of posts for each user
count = ownerId.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)

# combData = idRep.cogroup(count)

#join Id/Reputation RDD with Id/number of posts RDD
joinRdd = idRep.map(lambda x:(x[0],x[1])).join(count.map(lambda x:(x[0],x[1])))
# combData.map(lambda x: (list(x[1], x[0]), list(x[1], x[1])) )

# returns inner tuple consisting of (reputation, number of posts)
def iterate(iterable):
    r = []
    for v1_iterable in iterable:
        for v2 in v1_iterable:
            r.append(v2)

    return tuple(r)

# Hint: stored in format (Id, (Rep, noOfPost)). 
# E.g. (u'772521', (u'1538', 94)).
# idRepPostData = combData.mapValues(iterate)
# Extract values (tuples) from idRepPostData
# E.g. (u'9605', 383)
dataset = joinRdd.values() # <<- Use this dataset for cluster analysis.x

# train k-means algorithm
# centroid, centroid_history, point_owners = KMeans.train(dataset, k=3)
# Graph.plot(dataset, centroid_history, point_owners)

# sampleList = repPostData.takeSample(False, 100)
# sampleRdd = sc.parallelize(sampleList)
# sampleRdd.saveAsTextFile("so2017/takesample/cogrouped")
# for e in sampleRdd.collect():
    # print(e)

# print(combData.collect())

# Code below is yet to be audited.
#join Id/Reputation RDD with Id/number of posts RDD
# joinRdd = idRep.map(lambda x:(x[0],x[1])).join(count.map(lambda x:(x[0],x[1])))

#create RDD with values needed for clustering
# repNoPosts = joinRdd.map(lambda (k,v): v)

# print(joinRdd.take(10))
