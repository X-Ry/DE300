# Homework 2

The goal of the assignment is to
1. Use an S3 bucket containing the original data (My assumption is we should use our existing personal folders, I am using s3://de300spring2024/ryan_newkirk/)
2. Implement the cleaning steps
3. Split the data into training and testing with stratification on the labels
4. Train binary classificaiton models on the data, assessing performance using 5-fold cross-validation and reporting performance metrics
5. Selecting the final model

## Files in this folder

   - data -- folder that contains the raw data.

   - dockerfiles -- Put all dockerfiles for the project in this folder. In this case only one docker container is nessecary, 'dockerfile-jupyter' is used for creating a python image with required package that relates to the project.
   
   - run.sh -- This creates the docker volume, creates the image for the jupyter container, and runs the jupyter container
      
   - src -- This creates soource files and staging data.

## Instruction for deploying my HW2:

   - Run the run.sh file to create initiate all required containers(etl-container and postgres-container) and enter the shell of etl-container. 
   - Start the jupyter notebook by 'jupyter notebook --ip=0.0.0.0 --allow-root'. 
   - Run all cells in the jupyter notebook HW2.ipynb
