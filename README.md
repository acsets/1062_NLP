**The program is written in pyspark. Pyspark helps the performance by parallelization.**

# Usage
to run the code:  
`spark-submit token_type_count.py <path>/Texts_tokenized/ <path>/text_description/ 3 20`  
means input_path == <path>/Texts_tokenized/ and output_path == <path>/text_description/  
and get the first 20 tokens that occurs most frequently which are of length 3  

The last two arguments are optional.
