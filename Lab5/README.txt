Apache Spark Environment Selected:

 Use of Jupyter Notebook with PySpark on local machine.

Setup: 

Linux:
https://community.hortonworks.com/articles/75551/installing-and-exploring-spark-20-with-jupyter-not.html

Windows:
https://medium.com/@GalarnykMichael/install-spark-on-windows-pyspark-4498a5d8d66c


Steps to Execute Word Co-occurrence:

1. Setup spark on local machine with Jupyter.
2. Load the necessary imports(Lemmatizer.csv, *.tess files)
	Here. *.tess reads all the latin text files present in the directory.
3. Execute the notebook for n=2 and n=3 grams as desired.
4. The output of program is converted into an array and stored as text file also, it can be viewed on the Notebook in the given format.
5. Run the R notebook for the performance evaluation with different size of input and the corresponding time taken for execution.

Explanation of the output:

In case of 2/3 grams, the output file comprises of the word pair and the locations at which the words occur.

Eg: 

(('ille', 'qui'), '<ambrose. ap_david_altera. 2><ambrose. ap_david_altera. 12><ambrose. ap_david_altera. 15><ambrose. ap_david_altera. 19><ambrose. 						ap_david_altera. 25><ambrose. ap_david_altera. 38><ambrose. ap_david_altera. 39><ambrose. ap_david_altera. 42><ambrose. 								ap_david_altera. 43><ambrose. ap_david_altera. 57><ambrose. ap_david_altera. 59><ambrose. ap_david_altera. 60><ambrose. 								ap_david_altera. 67><cic. man. 57>')

Here, the word pair, "'ille', 'qui'" occurs at the above locations as shown.

