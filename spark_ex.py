

#load file into rdd
lines_rdd = sc.textFile("Complete_Shakespeare.txt")
#count number of lines
lines_rdd.count()
#split
words_rdd = lines_rdd.flatMap(lambda x: x.split())
#wordcount
words_rdd.count()
#distinct
words_rdd.distinct().count()
#mapping each with 1
key_value_rdd = words_rdd.map(lambda x: (x,1))
key_value_rdd.take(5)
#adding
word_counts_rdd = key_value_rdd.reduceByKey(lambda x,y: x+y)
word_counts_rdd.take(5)
#flipping for sort
flipped_rdd = word_counts_rdd.map(lambda x: (x[1],x[0]))
flipped_rdd.take(5)
#most freq words 5
results_rdd = flipped_rdd.sortByKey(False)
results_rdd.take(5)

