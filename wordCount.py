from pyspark import SparkContext 

sc = SparkContext(appName='WorkCount',master='local[*]').getOrCreate()


sc.textFile('story.txt').\
flatMap(lambda line: line.lower().split(' ')).\
map(lambda word: (word,1)).\
reduceByKey(lambda x,y:x+y).\
sortBy(lambda word: -word[1]).\
saveAsTextFile('word_count.txt')
