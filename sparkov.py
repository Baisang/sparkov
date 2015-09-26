from pyspark import SparkContext, SparkConf

appName = 'sparkov'
master = 'local[8]'
fileName = 'pride.txt'

conf = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=conf);

"""word_line is a list of words in the sentence like so:
['This', 'is', 'a', 'sentence.']
We return tuple mappings like so:
(('This', 'is'), 'a'))
in a list
"""
def create_tuples(word_line):
	end_list = []
	for i in range(len(word_line) - 2):
		tup = ((word_line[i], word_line[i+1]), word_line[i+2])
		end_list.append(tup)
	return end_list


dumpFile = sc.textFile(fileName)
dumpFile = dumpFile.map(lambda s: s.lower())
line_words = dumpFile.map(lambda s: s.split(" "))
tuples = line_words.flatMap(create_tuples).cache()
# I don't think this mapping maps K --> many V, so will have to change
markov_dict = tuples.collectAsMap()

# the following gets the counts of each tuple
tuple_count = tuples.map(lambda t: (t, 1))
count = tuple_count.reduceByKey(lambda a, b: a + b).cache()



count.saveAsTextFile(fileName+'.counts')


