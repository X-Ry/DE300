
### Ryan Newkirk HW4

My code is within the MWAA environment `de300spring2024-airflow-demo`, and the DAG name is `ryan_newkirk_hw4_6`. You can see it has run succesfully here:

![Screenshot from AWS](https://i.ibb.co/YTyPQq4/image.png)

My DAG code can be found in the `de300spring2024-airflow s3` bucket, under the filename `ryan_newkirk_hw4_6.py`. The entire airflow code is within this file.


I have also copied the data used for reference in the `data` folder.


### How I structured my code:

Task 1 - Read in Data from S3


Task 2 - Perform EDA, with spark and with standard pandas (without imputing spoke column)


Task 3 - Two separate Feature Engineering Strategies (imputing smoke column)


Task 3.5 - Perform Web Scraping --> merge the feature engineering variables with the web scraped data for smoke. for the non-merged paths, we need to remove the smoke variable.


Task 4 - Train two sklearn models (log regression and svm respectively), two Spark models, and two more sklearn models for the merged data. At this point, calculate evaluation metrics as well.


Task 5 - Compare the accuracy of each model and decide on a best one, using the eval metrics


Task 6 - return the Evaluation which we already ran, and print out the best performing model, as well as details on evaluation metrics.