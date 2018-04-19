#this program is written in pyspark syntax
from pyspark import SparkContext
sc = SparkContext()
import os
import sys

def main(in_path, out_path, token_length, rank_n):
    for file_name in os.listdir(in_path):
        open_file = open(in_path + file_name, mode = 'r')
        text = open_file.read()
        open_file.close()
        text_list = text.split("\n") #turn string into list
        rdd_text_list = sc.parallelize(text_list) #python list into RDD
        num_token = get_num_token(rdd_text_list)
        num_word_type = get_num_of_word_type(rdd_text_list)
        num_n_character_token, num_n_character_wordType = n_character_token_wordType_count(token_length, rdd_text_list)
        list_rank_most_frequent_token = token_frequency_rank(rank_n, rdd_text_list)
        description = edit_text_description(file_name, num_token, num_word_type, token_length, num_n_character_token, num_n_character_wordType, list_rank_most_frequent_token, rank_n)
        out_file = open(out_path + file_name, mode = 'w')
        out_file.write(description)
        out_file.close()

def edit_text_description(celebrity_name, num_token, num_word_type, token_length, num_n_char_token, num_n_character_wordType, list_ranking, rank_n):
    description =\
'''\
{} text description:
Number of token: {}
Number of word type: {}
Number of length-{} token: {}
Number of length-{} word type: {}
'''.format(celebrity_name, num_token, num_word_type, token_length, num_n_char_token, token_length, num_n_character_wordType)
    
    stringified_list_ranking = list(map(lambda key_value_pair_tuple: str(key_value_pair_tuple), list_ranking))
    str_ranking = "\n".join(stringified_list_ranking)
    description += "Top " + str(rank_n) + " frequent tokens: \n" + str_ranking + '\n'
    return description

def get_num_token(rdd_text_list):
    return rdd_text_list.count()

def n_character_token_wordType_count(n, rdd_text_list):#get back the number of tokens and word type that are length n
    rdd_n_char_token = rdd_text_list.filter(lambda token: len(token) == int(n) )
    n_char_token_count = rdd_n_char_token.count()
    rdd_word_occurence_key_value_tuple = get_token_occurence_key_value_tuple(rdd_n_char_token) 
    n_char_word_type_count = rdd_word_occurence_key_value_tuple.count()
    return n_char_token_count, n_char_word_type_count

def token_frequency_rank(n, rdd_text_list):#get back n highest frequency tokens
    rdd_word_occurence_key_value_tuple = get_token_occurence_key_value_tuple(rdd_text_list)
    top_n_frequent_tokens = rdd_word_occurence_key_value_tuple.sortBy(lambda key_value_tuple: key_value_tuple[1], ascending = False).take(int(n)) #rdd's take() method returns a list, a list of key-value pair tuple in this case. ex: [('年', 30), ('的', 28), ('為', 16), ('昭和', 15), ('臺灣', 14), ('臺', 13), (' 音樂', 12), ('陳', 12), ('在', 10), ('學校', 9)] 
    return top_n_frequent_tokens 
    
def get_token_occurence_key_value_tuple(rdd_text_list): #get back token and its number of occurence
    rdd_word_occurence = rdd_text_list.map(lambda x: (x,1)) 
    rdd_word_occurence_key_value_tuple = rdd_word_occurence.reduceByKey(lambda accu, n: accu + n)
    return rdd_word_occurence_key_value_tuple 

def get_num_of_word_type(rdd_text_list):
    return get_token_occurence_key_value_tuple(rdd_text_list).count()

if __name__ == "__main__":
    assert len(sys.argv) == 5, "token_type_count.py <directory path where tokenized texts are stored> <directory where the text description goes to> <word length> <top n rank>"
    if len(sys.argv) == 5: 
        main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
