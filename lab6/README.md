# Lab 6 Assignment

## What I Implemented and Instructions on How to Run:
Word Count can be run by building and starting the docker container with `bash run.sh`, entering the word-count folder, and then running `bash run-py-spark.sh`

Spark SQL can be run by starting the docker container with `bash run.sh`, entering the spark-sql folder, then running:
`source demos/bin/activate`
Then you can run this command to run the jupyter notebook: `jupyter notebook --ip=0.0.0.0 --allow-root`, which contains my completed code for the Spark-sql part of the assignment, the functions added are "ages_30_to_50" which address #1, and  "store_pyspark_dataframe_as_csv" which addresses #2.  These same functions are included in process.py, and I wrote "store_pyspark_dataframe_as_csv" separately as during lab it was mentioned that #2 should be for all of the data, not just the subset of ages 30-50. The process.py file can be run using the bash script `bash run-py-spark.sh`.
  
## Word Count

1 Save only the words that have count greater or equal to 3.

## Spark-sql
1, Add one more cell in ./spark-sql/pyspark-sql.ipynb that select rows with 'age' between 30 and 50 (inclusive) and transforms the selected pyspark dataframe into pandas dataframe and print out the summary statistics using 'describe()'.

2, Wrap all functions in the ./spark-sql/pyspark-sql.ipynb notebook into a .py file and write a run-py-spark.sh file (similar to the one in the word-counts folder). \
When you run 'bash run-py-spark.sh', the .py file should be executed with Spark (i.e including the steps of data reading data, data cleaning, data Transformation, but without the step in task 1 above), and the final dataframe should be stored as .csv file in the './data' folder.