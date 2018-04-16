#this program is written in pyspark syntax
from pyspark import SparkContext
sc = SparkContext()
import os
import sys

def main(in_path, out_path, token_length, rank_n):
    for file_name in os.listdir(in_path):
        open_file = open(in_path + file_name, mode = 'r')
        text = open_file.read()
        text_list = text.split("\n") #turn string into list
        rdd_text_list = sc.parallelize(text_list) #python list into RDD
        num_token = token_count(rdd_text_list)
        num_word_type = word_type_count(rdd_text_list)

        if token_length != None:
            num_n_character_token, num_n_character_wordType = n_character_token_wordType_count(token_length, rdd_text_list)
        else: # token_length argument not given in the command line
            num_n_character_token = None
        if rank_n != None:
            list_rank_most_frequent_token = token_frequency_rank(rank_n, rdd_text_list)
        else: # rank_n argument not given in the command line
            list_rank_most_frequent_token = None
        
        description = edit_text_description(file_name, num_token, num_word_type, token_length, num_n_character_token, num_n_character_wordType, list_rank_most_frequent_token, rank_n)
        out_file = open(out_path + file_name, mode = 'w')
        out_file.write(description)
        
def edit_text_description(celebrity_name, num_token, num_word_type, token_length, num_n_char_token, num_n_character_wordType, list_ranking, rank_n):
    description = celebrity_name + " text description: \n"
    description += "Number of token: " + str(num_token) + '\n'
    description += "Number of word type: " + str(num_word_type) + '\n'
    description += "Number of length-" + str(token_length) + " token: " + str(num_n_char_token) + '\n'
    description += "Number of length-" + str(token_length) + " word type: " + str(num_n_character_wordType) + '\n'
    if list_ranking != None:
        stringified_list_ranking = list(map(lambda key_value_pair_tuple: str(key_value_pair_tuple), list_ranking))
        str_ranking = "\n".join(stringified_list_ranking)
        description += "Top " + str(rank_n) + " frequent tokens: \n" + str_ranking + '\n'
    return description
 
def token_count(rdd_text_list):
    rdd_token = rdd_text_list.map(lambda token: 1) 
    token_count = rdd_token.reduce(lambda accu,n: accu+n) 
    return token_count

    #get back the number of tokens that are length n, and the word type that are length n
def n_character_token_wordType_count(n, rdd_text_list): 
    rdd_n_char_token = rdd_text_list.filter(lambda token: len(token) == int(n) )
    rdd_n_char_token_mapped_to_1 = rdd_n_char_token.map(lambda n_char_token: 1)
    if rdd_n_char_token_mapped_to_1.count() > 0: #since it will cause error if reducing an empty RDD
        n_char_token_count = rdd_n_char_token_mapped_to_1.reduce(lambda accu, n: accu+n)
    else:
        n_char_token_count = 0
    rdd_word_occurence_key_value_tuple = token_occurence(rdd_n_char_token) 
    rdd_mapped_to_1 = rdd_word_occurence_key_value_tuple.map(lambda key_value_tuple: 1)
    n_char_word_type_count = rdd_mapped_to_1.reduce(lambda x,y: x+y)
    return n_char_token_count, n_char_word_type_count

def token_frequency_rank(n, rdd_text_list): #get back n highest frequency tokens
    rdd_word_occurence_key_value_tuple = token_occurence(rdd_text_list)
    rdd_sorted_descending = rdd_word_occurence_key_value_tuple.sortBy(lambda key_value_tuple: key_value_tuple[1], ascending = False)
    return rdd_sorted_descending.take(int(n)) 

def token_occurence(rdd_text_list): #get back token and its number of occurence
    rdd_word_occurence = rdd_text_list.map(lambda x: (x,1)) 
    rdd_word_occurence_key_value_tuple = rdd_word_occurence.reduceByKey(lambda accu, n: accu + n)
    return rdd_word_occurence_key_value_tuple 

def word_type_count(rdd_text_list): #get back the number of word type
    rdd_word_occurence_key_value_tuple = token_occurence(rdd_text_list)
    rdd_word_type = rdd_word_occurence_key_value_tuple.map(lambda x: 1) 
    word_type_count = rdd_word_type.reduce(lambda accu, n: accu + n)
    return word_type_count

if __name__ == "__main__":
    if len(sys.argv) == 5: 
        main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
    elif len(sys.argv) == 4: 
        main(sys.argv[1], sys.argv[2], sys.argv[3], None)
    elif len(sys.argv) == 3: 
        main(sys.argv[1], sys.argv[2], None , None)
    else:
        print("the number of argument is either too large or too small")
