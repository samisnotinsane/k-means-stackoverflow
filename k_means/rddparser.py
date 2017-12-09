from pyspark import SparkContext, SparkConf, SQLContext
from time import time
from pprint import pprint
from numpy import array

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
count = ownerId.map(lambda x: (x[0], 1)).reduceByKey(lambda x, y: 1 + y)
 
#join Id/Reputation RDD with Id/number of posts RDD
joinRdd = idRep.map(lambda x:(x[0],x[1])).join(count.map(lambda x:(x[0],x[1])))

#create RDD with values needed for clustering
repNoPosts = joinRdd.map(lambda (k,v): v)

print(joinRdd.take(100))
