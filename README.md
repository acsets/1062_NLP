**The program is written in pyspark. Pyspark helps the performance by parallelization.**

# Spark Intro
Spark is an engine that helps automatically distribute computation jobs to nodes(cores).

# Spark Installation
Download spark-2.3.0-bin-hadoop2.7.tgz from Spark officail website: https://spark.apache.org/downloads.html  
After downloading, extract it and you are ready to go. You will need to install java if java is not pre-installed.  
**Ubuntu is recommended.**

# Usage
cd to <path_to_your_spark>/bin, and run the following command    
`spark-submit token_type_count.py <path>/Texts_tokenized/ <path>/text_description/ 3 20`   
The arguments are input path, output path, word length, number of ranking. In this example, the program will get back the top 20 tokens that occurs most frequently and the token occurence and word type of length-3 words.  
To get the whole distribution of token occurence, set number of ranking to be the number of word_type.  

The last two arguments are optional.
