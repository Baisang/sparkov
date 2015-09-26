from pyspark import SparkContext, SparkConf

appName = 'sparkov'
master = 'local[8]'
fileName = ''

conf = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=conf);

"""word_line is a list of words in the sentence like so:
['This', 'is', 'a', 'sentence.']
We return tuple mappings like so:
((('This', 'is'), 'a'), 1)
in a list
"""
def create_tuples(word_line):
	end_list = []
	for i in range(len(word_line) - 2):
		tup = (((word_line[i], word_line[i+1]), word_line[i+2]), 1)
		end_list.append(tup)
	return end_list


dumpFile = sc.textFile(fileName)
dumpFile = dumpFile.map(lambda s: s.lower())
line_words = dumpFile.map(lambda s: s.split(" "))
tuples = line_words.flatMap(create_tuples)
reduced = tuples.reduceByKey(lambda a, b: a + b)
reduced.saveAsTextFile(fileName+'.reduced')


