#this program is written in pyspark syntax
from pyspark import SparkContext
sc = SparkContext()
import os
import sys

def main(directory_path):
    tokenization_statistic = ""
    for file_name in os.listdir(directory_path):
        open_file = open(directory_path + file_name, mode = 'r')
        text = open_file.read()
        text_list = text.split("\n")
        rdd_text_list = sc.parallelize(text_list)
        num_token = token_count(rdd_text_list)
        num_word_type = word_type_count(rdd_text_list)
        tokenization_statistic = tokenization_statistic + file_name + "-> numberof token: " + str(num_token) + ", number of word type: " + str(num_word_type) + '\n'
    print(tokenization_statistic)

def token_count(rdd_text_list):
    rdd_token = rdd_text_list.map(lambda x: 1) 
    token_count = rdd_token.reduce(lambda accu,n: accu+n) #accu means acculmulator
    return token_count

def word_type_count(rdd_text_list): 
    #count each word's occurence
    rdd_word_occurence = rdd_text_list.map(lambda x: (x,1)) #turn into key-value pairs
    word_occurence_count = rdd_word_occurence.reduceByKey(lambda accu, n: accu + n) #ex: turn [('臺北', 1), ('臺南', 1), ('臺北', 1), ('臺北', 1), ('臺南', 1)] into [ ('臺北', 3),  ('臺南', 2)]

    #count how many unique words are in this text
    rdd_word_type = word_occurence_count.map(lambda x: 1) 
    word_type_count = rdd_word_type.reduce(lambda accu, n: accu + n)
    return word_type_count

if __name__ == "__main__":
    assert len(sys.argv) == 2, "token_type_count.py <directory_path_where_tokenized_texts_are_stored>"
    main(sys.argv[1])
