from pyspark import SparkContext, SparkConf

_fileName = ''
sc = None

"""word_line is a list of words in the sentence like so:
['This', 'is', 'a', 'sentence.']
We return tuple mappings like so:
(('This', 'is'), 'a'))
in a list
"""
def _create_tuples(word_line):
	end_list = []
	for i in range(len(word_line) - 2):
		tup = ((word_line[i], word_line[i+1]), word_line[i+2])
		end_list.append(tup)
	return end_list


""" This function must be called first to initialize the SparkContext.
"""
def configure(appName, master, fileName):
	_fileName = fileName
	conf = SparkConf().setAppName(appName).setMaster(master)
	sc = SparkContext(conf=conf)

"""Creates a dictionary mapping each pair of adjacent words to the following word.
If the optional parameters writeToFile and destination are specified, will also
write out to a file the pairings along with the count for each mapping.
"""
def create_dictionary(writeToFile=False, destination=None):
	dumpFile = sc.textFile(_fileName)
	dumpFile = dumpFile.map(lambda s: s.lower())
	line_words = dumpFile.map(lambda s: s.split(" "))
	tuples = line_words.flatMap(_create_tuples).cache()
	# map tuple (('This', 'is'), 'a')) to (('This', 'is'), ['a'])
	# so then when adding with another tuple with key ('This', 'is'),
	# can just concat the list value
	tuple_map = tuples.map(lambda t: (t[0], [t[1]]))
	tuple_map = tuple_map.reduceByKey(lambda a, b: a + b)
	# turn into a dictionary!!
	tuple_dict = tuple_map.collectAsMap()

	if writeToFile and destination:
		# the following gets the counts of each tuple
		tuple_count = tuples.map(lambda t: (t, 1))
		count = tuple_count.reduceByKey(lambda a, b: a + b).cache()
		count.saveAsTextFile(destination)

	return tuple_dict