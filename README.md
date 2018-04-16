**The program is written in pyspark. Pyspark helps the performance by parallelization.**

# Usage
to run the code:  
`spark-submit token_type_count.py <path>/Texts_tokenized/ <path>/text_description/ 3 20`   
The arguments are input path, output path, word length, number of ranking  
In this example, will get back the first 20 tokens that occurs most frequently  
and the token occurence and word type of length-3 words.

The last two arguments are optional.
