1. WORD COUNT ON TWEETS:

First, we collect tweeets on soccer data and clean them up in Jupyter. we create two text files one named 'soccerhashuser.txt' which contains the user mentions(@word) and hashtags(#tags) and 'soccertext.txt' which contains the text of tweets which is used as input for part 2.

Next, run the jar 'wc.jar' in hadoop VM by following the sequence(as mentioned in MR Guide):

Compile:
hadoop com.sun.tools.javac.Main WordCount.java
generate jar:
jar cf wc.jar WordCount*.class
put input soccerhashuser.txt in input directory and run to put input folder in that directory(dic here):
hfs dfs -put dic/input

run jar:
hadoop jar wc.jar WordCount input/dic output1
get output
hfs dfs -get output1

check folder output1 for output.

Finally plot the output as word cloud in R.

2. Word co - occurrence on tweets :

Run the Pairs and stripes jar as per the command mentioned above:
hadoop jar pair.jar Pairs input/dic outputPair
hadoop jar stripe.jar Stripes input/dic outputStripes

here dic directory contains soccertext.txt 

I haven’t cleaned much data in Pairs method. More data cleaning is done in Stripe method. Both produce co occurence of 2 words.


Featured  Activity  1: 
Wordcount  on  Classical  Latin  text :


Here the jar lemma.jar can be executed using the input folder as mentioned in the zip file provided n the output is produces as mentioned.

Algorithm is the one provided in pdf.



Featured
Activity 2: 
Word co-occurrence among multiple documents

First, we get the data then generate line tokens and add it in hash map. Later we fetch for lemmas corresponding to neighbor and get the location in the end the output produces is in the form as desired. 

Here I have implemented 2 grams and three grams for a max of 10.6mb file data comprising of 12 files and . the input files and output files are attached as mentioned in the input and output folders.

Both the above activities can be executed as mentioned above after shifting input to respective folders and executing jar with proper names.

A final plot of performance and scalability in terms of size and time taken for 2 and 3 grams is plotted. 


Lemmatizer used file fro two activities is new_lemmatizer.csv

lemma.jar for activity1
lemma2.jar for 2grams activity2
lemma3.jar for 3grams activity2.


References:
https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html



