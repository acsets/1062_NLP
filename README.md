**The program is written in pyspark. Pyspark helps the performance by parallelization.**

# Usage
to run the code:  
`spark-submit token_type_count.py <path>/Texts_tokenized/ <path>/text_description/ 3 20`   
The arguments are input path, output path, word length, number of ranking  
In this example, will get back the top 20 tokens that occurs most frequently  
and the token occurence and word type of length-3 words.
To get whole distribution of token occurence, set number of ranking be the number of word_type.  

The last two arguments are optional.
