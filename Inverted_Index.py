from pyspark import SparkContext
import argparse


" this illustrate basic idea in search engines "

def search(input_path, search_string):
    sc = SparkContext("local", "Inverted Index")

    "build the inverted index"
    inverted_index_rdd = sc.wholeTextFiles(input_path)
    inverted_index_rdd =inverted_index_rdd.flatMap(lambda (file, content): map(lambda word: (word,[file]), set(content.lower().split())))
    inverted_index_rdd = inverted_index_rdd.reduceByKey(lambda file1, file2: file1 + file2)

    inverted_index_rdd2 = inverted_index_rdd.filter(lambda (word, file_list): len(file_list) == 14)
    inverted_index_rdd3 = inverted_index_rdd2.sortBy((lambda pair: pair[0]), ascending = True)
    words = []
    for pair in inverted_index_rdd3.collect():
        words.append(pair[0])
    " , ".join(words)