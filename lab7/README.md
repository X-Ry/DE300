# Lab 7 Assignment

## My Work

My work on Lab 7 is included in classify.py and ml_pyspark.ipynb. You can see the results after running the model saved in the jupyter notebook.

### Original Results before implementing Step 1 & 2:

Area Under ROC Curve: 0.8662

Selected Maximum Tree Depth: 6

### New Results after implementing Squared Features and Tree Depth:

Area Under ROC Curve: 0.8807

Selected Maximum Tree Depth: 4

### My Analysis

It seems like this increase in AUC ROC indicates a more robust and generalizable model. This is in part due to the squared features contributing to capturing non-linear relationships more effectively, and the change in number of trees and the subsequent reduction in tree depth may have helped in avoiding overfitting and better generalization. Overall these modifications that I made seem to help improve model accuracy in predictions!


## Machine learning with Spark

1 In the jupyter notebook 'ml_pyspark.ipynb', the PairwiseProduct step in the pipeline adds the cross product of any two numerical features to the dataframe. (e.g, if there are three variables a,b,c, then the PairwiseProduct step adds cross products a*b, a*c, b*c). In this part, you are required to also add the squared of numerical features to the dataframe (following the last example, you want to add a^2, b^2, c^2 to the dataframe). You should create a new Transofomer class for this purpose and add it into the pipeline.

2 The Cross Validation selects the maximum tree depth for the random forest model. There are many other hyper-parameters for the random forest. Search what they are, choose one such hyperparameter with its reasonble values and add to the ParameterGrid. 

3 Wrap all the functions you have into a python script (including the modification you have made to 1,2 above), and write a .sh file to execute the python script with EMR(spark on AWS, see the ppt tutorial in the folder spark-on-AWS). 

4 The python script should print out the new AUC ROC, Report and analyze what you find (compared to w.o modification of 1,2 above)


