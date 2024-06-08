from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import boto3
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, mean, median, monotonically_increasing_id, lit, udf
from pyspark.sql.types import IntegerType, FloatType, StringType, StructType, StructField
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression as PySparkLogisticRegression
from pyspark.ml.classification import RandomForestClassifier, LinearSVC, DecisionTreeClassifier, GBTClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import Row
import random


from sklearn.linear_model import LogisticRegression
from sklearn.svm import SVC
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import accuracy_score, classification_report
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, make_scorer

WORKFLOW_SCHEDULE_INTERVAL = "@daily"

# Define the default args dictionary
default_args = {
    'owner': 'RyanNewkirk',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}


def eda(**kwargs):
    df = pd.read_csv("/tmp/heart_disease.csv")
    
    # Define threshold for missing percentage
    threshold = 0.9

    # Remove rows
    df_clean = df[df.isnull().mean(axis=1) < threshold]
        
    # Define a list that contains the column names you want to keep
    cols_to_keep = ['age', 'sex', 'painloc', 'painexer', 'cp', 'trestbps', 'smoke', 'fbs', 'prop', 
                    'nitr', 'pro', 'diuretic', 'thaldur', 'thalach', 'exang', 'oldpeak', 'slope', 'target']

    # Select only the columns of interest
    df_step1 = df_clean[cols_to_keep]
    # We can just keep the mode values for painloc and painexer, since these are binary values that would make the most sense for imputation
    painloc_mode = df_step1['painloc'].dropna().mode()[0]
    df_step1['painloc'].fillna(painloc_mode, inplace=True)
    painexer_mode = df_step1['painexer'].dropna().mode()[0]
    df_step1['painexer'].fillna(painexer_mode, inplace=True)

    # Calculate the mean for 'trestbps' column, to impute values less than 100 or null values
    trestbps_mean = df_step1['trestbps'][df_step1['trestbps'] >= 100].mean()
    df_step1.loc[df_step1['trestbps'] < 100, 'trestbps'] = trestbps_mean
    df_step1['trestbps'].fillna(trestbps_mean, inplace=True)
    #Do the same for oldpeak
    oldpeak_mean = df_step1['oldpeak'][(df_step1['oldpeak'] >= 0) & (df_step1['oldpeak'] <= 4)].mean()
    df_step1.loc[(df_step1['oldpeak'] < 0) | (df_step1['oldpeak'] > 4), 'oldpeak'] = oldpeak_mean
    df_step1['oldpeak'].fillna(oldpeak_mean, inplace=True)

    # impute with mean values for thaldur, thalach
    painloc_mode = df_step1['thaldur'].dropna().mean()
    df_step1['thaldur'].fillna(painloc_mode, inplace=True)
    painexer_mode = df_step1['thalach'].dropna().mean()
    df_step1['thalach'].fillna(painexer_mode, inplace=True)

    # fbs, prop, nitr, pro, diuretic: Replace the missing values and values greater than 1. This includes exang too as a binary variable
    binary_columns = ['fbs', 'prop', 'nitr', 'pro', 'diuretic', 'exang']
    for col in binary_columns:
        col_mode = df_step1[col][df_step1[col] <= 1].mode()[0]
        df_step1.loc[df_step1[col] > 1, col] = col_mode
        df_step1[col].fillna(col_mode, inplace=True)

    # Since 'slope' and 'cp' are categorical data, we will impute using the mode
    slope_mode = df_step1['slope'].dropna().mode()[0]
    df_step1['slope'].fillna(slope_mode, inplace=True)
    slope_mode = df_step1['cp'].dropna().mode()[0]
    df_step1['cp'].fillna(slope_mode, inplace=True)

    df_step1['age'] = pd.to_numeric(df_step1['age'], errors='coerce')

    output_path = "/tmp/heart_disease_eda.csv"
    df_step1.to_csv(output_path, index=False)

    return output_path


def spark_eda(**kwargs):
    spark = SparkSession.builder.appName("HW4").getOrCreate()
    df = spark.read.csv("/tmp/heart_disease.csv", header=True, inferSchema=True)

    total_columns = len(df.columns)

    # Define a threshold for the maximum allowed percentage of missing values
    max_missing_threshold = 0.9

    # Calculate the percentage of missing values for each row
    df_with_missing_percentage = df.withColumn('missing_percentage', sum(when(col(c).isNull(), 1).otherwise(0) for c in df.columns) / lit(total_columns))

    # Filter rows where the percentage of missing values is less than or equal to the threshold
    df_clean = df_with_missing_percentage.filter(col('missing_percentage') <= max_missing_threshold)

    # Drop the 'missing_percentage' column as it's no longer needed
    df_clean = df_clean.drop('missing_percentage')
    
    # Select only the columns of interest
    cols_to_keep = ['age', 'sex', 'painloc', 'painexer', 'cp', 'trestbps', 'smoke', 'fbs', 'prop', 
                    'nitr', 'pro', 'diuretic', 'thaldur', 'thalach', 'exang', 'oldpeak', 'slope', 'target']
    df_step1 = df_clean.select(*cols_to_keep)

    # Fill missing values with mode for binary columns
    binary_columns = ['painloc', 'painexer', 'fbs', 'prop', 'nitr', 'pro', 'diuretic', 'exang']
    for col_name in binary_columns:
        mode_row = df_step1.groupBy(col_name).count().orderBy('count', ascending=False).first()
        if mode_row is not None:
            mode_value = mode_row[0]
            df_step1 = df_step1.na.fill({col_name: mode_value})

    # Fill missing or incorrect values with mean for continuous columns
    continuous_columns = ['trestbps', 'oldpeak', 'thaldur', 'thalach']
    for col_name in continuous_columns:
        mean_value = df_step1.select(mean(col_name).alias('mean')).collect()[0]['mean']
        df_step1 = df_step1.withColumn(col_name, when(col(col_name).isNull() | (col(col_name) < lit(100)), mean_value).otherwise(col(col_name)))

    # Impute missing values with mode for categorical columns
    categorical_columns = ['slope', 'cp']
    for col_name in categorical_columns:
        mode_row = df_step1.groupBy(col_name).count().orderBy('count', ascending=False).first()
        if mode_row is not None:
            mode_value = mode_row[0]
            df_step1 = df_step1.na.fill({col_name: mode_value})

    # Cast 'age' to numeric and handle non-numeric entries
    df_step1 = df_step1.withColumn('age', col('age').cast('float'))

    # Save the DataFrame to a CSV file
    output_path = "/tmp/heart_disease_spark_eda.csv"
    df_step1.write.csv(output_path, header=True, mode='overwrite')

    # Stop the Spark session
    spark.stop()
    return output_path

def fe1(**kwargs):
    print("Feature Engineering 1 with spark eda")
    spark = SparkSession.builder.appName("HW4").getOrCreate()
    df = spark.read.csv("/tmp/heart_disease_spark_eda.csv", header=True, inferSchema=True)
    #Similar to Lab 7, we can add new features using squared values.
    df = df.withColumn('oldpeak_squared', col('oldpeak') ** 2)
    #Add a monotomically increasing id, so we will be able to merge/join datasets when we need to
    df = df.withColumn('id', monotonically_increasing_id())
    
    out_path = "/tmp/heart_disease_fe1.csv"
    df.write.csv(out_path, header=True, mode='overwrite')
    # Stop the Spark session
    spark.stop()
    return out_path

def fe2(**kwargs):
    print("Feature Engineering 2 with eda")
    df = pd.read_csv("/tmp/heart_disease_eda.csv")
    #Similar to Lab 7, we can add new features using squared values.
    df['age_squared'] = df['age'] ** 2
    #Add an id, so we will be able to merge/join datasets when we need to
    df['id'] = df.index

    out_path = "/tmp/heart_disease_fe2.csv"
    df.to_csv(out_path, index=False)
    return out_path
    
def merge(**kwargs):
    spark = SparkSession.builder.appName("HW4").getOrCreate()
    df_fe1 = spark.read.csv("/tmp/heart_disease_fe1.csv", header=True, inferSchema=True).toPandas()

    df_fe2 = pd.read_csv("/tmp/heart_disease_fe2.csv")

    df_fe1.columns = [col.replace('"', '') for col in df_fe1.columns]
    duplicate_cols = set(df_fe2.columns).intersection(set(df_fe1.columns)) - {'id'}
    df_fe1 = df_fe1.drop(columns=duplicate_cols)
    df_merged = pd.merge(df_fe2, df_fe1, on='id', how='inner')

    #Web Scraping
    df_gender_smoke_cdc = pd.read_csv("/tmp/gender_smoke_cdc.csv")
    df_age_smoke_cdc = pd.read_csv("/tmp/age_smoke_cdc.csv")
    df_smoke_dict_abs = pd.read_csv("/tmp/smoke_dict_abs.csv")
    gender_smoke_dict = pd.Series(df_gender_smoke_cdc.percentage.values, index=df_gender_smoke_cdc.gender).to_dict()
    age_smoke_dict = pd.Series(df_age_smoke_cdc.percentage.values, index=df_age_smoke_cdc.age_range).to_dict()
    smoke_abs_dict = pd.Series(df_smoke_dict_abs.percentage.values, index=df_smoke_dict_abs.age_range).to_dict()
        
    # Define a function to impute the 'smoke' column
    def impute_smoke(row):
        sex, age = row['sex'], row['age']
        # Determine the age range
        if age >= 18 and age <= 24:
            age_range_cdc = '18–24 years'
            age_range_abs = '18-24'
        elif age >= 25 and age <= 44:
            age_range_cdc = '25–44 years'
            age_range_abs = '25-34' if age <= 34 else '35-44'
        elif age >= 45 and age <= 64:
            age_range_cdc = '45–64 years'
            age_range_abs = '45-54' if age <= 54 else '55-64'
        elif age >= 65:
            age_range_cdc = '65 years and older'
            age_range_abs = '65-74' if age <= 74 else '75+'
        else:
            age_range_abs = '15-17'
        
        # Get the smoking probabilities
        prob_cdc = age_smoke_dict.get(age_range_cdc, 0)
        prob_abs = smoke_abs_dict.get(age_range_abs, 0)
        
        # Adjust probabilities by gender
        if sex == 'male':
            prob_cdc *= gender_smoke_dict.get('male', 0) / gender_smoke_dict.get('female', 1)
        
        # Calculate the final smoking probability
        final_prob = max(prob_cdc, prob_abs)
        
        # Randomly decide if the person is a smoker based on the final probability
        return 1 if random.random() < final_prob else 0

    df_merged['smoke_imputed'] = df_merged.apply(lambda row: impute_smoke(row) if pd.isnull(row['smoke']) else row['smoke'], axis=1)
    df_merged.drop('smoke', axis=1, inplace=True)
    df_merged.rename(columns={'smoke_imputed': 'smoke'}, inplace=True)

    out_path = '/tmp/heart_disease_merge.csv'
    df_merged.to_csv(out_path, index=False)
    spark.stop()
    return out_path

def remove_smoke1(**kwargs):
    spark = SparkSession.builder.appName("HW4").getOrCreate()
    
    df_fe1 = spark.read.csv("/tmp/heart_disease_fe1.csv", header=True, inferSchema=True)
    
    df_fe1 = df_fe1.drop('smoke')
    
    # Drop columns that contain any null values
    for col in df_fe1.columns:
        # Count the number of nulls in the column
        null_count = df_fe1.filter(df_fe1[col].isNull()).count()
        if null_count > 0:
            # Drop the column if any null value is found
            df_fe1 = df_fe1.drop(col)

    out_path = '/tmp/heart_disease_remove_smoke1.csv'
    
    df_fe1.write.csv(out_path, header=True, mode='overwrite')
    
    spark.stop()
    
    return out_path

def remove_smoke2(**kwargs):
    df_fe2 = pd.read_csv("/tmp/heart_disease_fe2.csv")

    df_fe2.drop('smoke', axis=1, inplace=True)

    out_path = '/tmp/heart_disease_remove_smoke2.csv'

    df_fe2.to_csv(out_path, index=False)

    return out_path

def save_metrics_to_csv(model_name, accuracy, precision, recall, f1_score):
    # Create a DataFrame with the metrics
    df_metrics = pd.DataFrame({
        'Model': [model_name],
        'Accuracy': [accuracy],
        'Precision': [precision],
        'Recall': [recall],
        'F1 Score': [f1_score]
    })
    out_path = f'/tmp/{model_name}_evaluation.csv'
    # Save the DataFrame to a CSV file
    df_metrics.to_csv(out_path, index=False)

def lr1(**kwargs):
    spark_session = SparkSession.builder.appName("HW4").getOrCreate()
    heart_disease_df = spark_session.read.csv("/tmp/heart_disease_remove_smoke1.csv", header=True, inferSchema=True)
    feature_columns = heart_disease_df.columns
    feature_columns.remove('target')
    vector_assembler = VectorAssembler(inputCols=feature_columns, outputCol='features')
    training_df, testing_df = heart_disease_df.randomSplit([0.9, 0.1], seed=42)
    logistic_regression_model = PySparkLogisticRegression(labelCol='target', featuresCol='features', maxIter=100)
    pipeline = Pipeline(stages=[vector_assembler, logistic_regression_model])
    fitted_model = pipeline.fit(training_df)
    prediction_df = fitted_model.transform(testing_df)

    # Create evaluators for each metric
    accuracy_evaluator = MulticlassClassificationEvaluator(labelCol='target', metricName='accuracy')
    precision_evaluator = MulticlassClassificationEvaluator(labelCol='target', metricName='weightedPrecision')
    recall_evaluator = MulticlassClassificationEvaluator(labelCol='target', metricName='weightedRecall')
    f1_evaluator = MulticlassClassificationEvaluator(labelCol='target', metricName='f1')

    # Evaluate each metric
    model_accuracy = accuracy_evaluator.evaluate(prediction_df)
    model_precision = precision_evaluator.evaluate(prediction_df)
    model_recall = recall_evaluator.evaluate(prediction_df)
    model_f1 = f1_evaluator.evaluate(prediction_df)

    save_metrics_to_csv('lr1', model_accuracy, model_precision, model_recall, model_f1)

    # Stop the Spark session
    spark_session.stop()
    return model_accuracy



def svm1(**kwargs):
    spark_session = SparkSession.builder.appName("HW4").getOrCreate()
    heart_disease_df = spark_session.read.csv("/tmp/heart_disease_remove_smoke1.csv", header=True, inferSchema=True)
    feature_columns = heart_disease_df.columns
    feature_columns.remove('target')
    vector_assembler = VectorAssembler(inputCols=feature_columns, outputCol='features')
    training_df, testing_df = heart_disease_df.randomSplit([0.9, 0.1], seed=42)
    linear_svc_model = LinearSVC(labelCol='target', featuresCol='features', maxIter=100)
    pipeline = Pipeline(stages=[vector_assembler, linear_svc_model])
    fitted_model = pipeline.fit(training_df)
    prediction_df = fitted_model.transform(testing_df)

    # Create evaluators for each metric
    accuracy_evaluator = MulticlassClassificationEvaluator(labelCol='target', metricName='accuracy')
    precision_evaluator = MulticlassClassificationEvaluator(labelCol='target', metricName='weightedPrecision')
    recall_evaluator = MulticlassClassificationEvaluator(labelCol='target', metricName='weightedRecall')
    f1_evaluator = MulticlassClassificationEvaluator(labelCol='target', metricName='f1')

    # Evaluate each metric
    model_accuracy = accuracy_evaluator.evaluate(prediction_df)
    model_precision = precision_evaluator.evaluate(prediction_df)
    model_recall = recall_evaluator.evaluate(prediction_df)
    model_f1 = f1_evaluator.evaluate(prediction_df)

    save_metrics_to_csv('svm1', model_accuracy, model_precision, model_recall, model_f1)

    spark_session.stop()

    return model_accuracy

def lr2(**kwargs):
    heart_disease_df = pd.read_csv("/tmp/heart_disease_remove_smoke2.csv")
    feature_columns = heart_disease_df.columns.tolist()
    feature_columns.remove('target')
    X_features = heart_disease_df[feature_columns]
    y_target = heart_disease_df['target']
    
    # Splitting the dataset
    X_train_set, X_test_set, y_train_set, y_test_set = train_test_split(X_features, y_target, test_size=0.1, random_state=42)
    
    # Initializing the Logistic Regression model
    logistic_regression_model = LogisticRegression(max_iter=1000, random_state=42)
    
       # Training the model
    logistic_regression_model.fit(X_train_set, y_train_set)

    # Using cross_val_score for evaluation
    scoring_metrics = {'accuracy': make_scorer(accuracy_score),
                       'precision': make_scorer(precision_score, average='weighted'),
                       'recall': make_scorer(recall_score, average='weighted'),
                       'f1': make_scorer(f1_score, average='weighted')}
    
    metrics_results = {}
    for metric_name, scorer in scoring_metrics.items():
        scores = cross_val_score(logistic_regression_model, X_features, y_target, cv=5, scoring=scorer)
        avg_score = scores.mean()
        metrics_results[metric_name] = avg_score
    
    # Save the metrics to a CSV file
    save_metrics_to_csv('lr2', 
                        metrics_results['accuracy'], 
                        metrics_results['precision'], 
                        metrics_results['recall'], 
                        metrics_results['f1'])
    
def svm2(**kwargs):
    heart_disease_df = pd.read_csv("/tmp/heart_disease_remove_smoke2.csv")
    feature_columns = heart_disease_df.columns.tolist()
    feature_columns.remove('target')
    X_features = heart_disease_df[feature_columns]
    y_target = heart_disease_df['target']
    
    # Splitting the dataset
    X_train_set, X_test_set, y_train_set, y_test_set = train_test_split(X_features, y_target, test_size=0.1, random_state=42)
    
    # Initializing the SVM classifier
    svm_classifier = SVC(random_state=42)
    
    # Training the classifier
    svm_classifier.fit(X_train_set, y_train_set)
    
    # Using cross_val_score for evaluation
    scoring_metrics = {'accuracy': make_scorer(accuracy_score),
                       'precision': make_scorer(precision_score, average='weighted', zero_division=0),
                       'recall': make_scorer(recall_score, average='weighted'),
                       'f1': make_scorer(f1_score, average='weighted')}
    
    metrics_results = {}
    for metric_name, scorer in scoring_metrics.items():
        scores = cross_val_score(svm_classifier, X_features, y_target, cv=5, scoring=scorer)
        avg_score = scores.mean()
        metrics_results[metric_name] = avg_score
    
    # Save the metrics to a CSV file
    save_metrics_to_csv('svm2', 
                        metrics_results['accuracy'], 
                        metrics_results['precision'], 
                        metrics_results['recall'], 
                        metrics_results['f1'])
    

def lr3(**kwargs):
    heart_disease_df = pd.read_csv("/tmp/heart_disease_merge.csv")
    feature_columns = heart_disease_df.columns.tolist()
    feature_columns.remove('target')
    X_features = heart_disease_df[feature_columns]
    y_target = heart_disease_df['target']
    
    # Splitting the dataset
    X_train_set, X_test_set, y_train_set, y_test_set = train_test_split(X_features, y_target, test_size=0.1, random_state=42)
    
    # Initializing the Logistic Regression model
    logistic_regression_model = LogisticRegression(max_iter=1000, random_state=42)
    
    # Training the model
    logistic_regression_model.fit(X_train_set, y_train_set)
    
    # Using cross_val_score for evaluation
    scoring_metrics = {'accuracy': make_scorer(accuracy_score),
                       'precision': make_scorer(precision_score, average='weighted'),
                       'recall': make_scorer(recall_score, average='weighted'),
                       'f1': make_scorer(f1_score, average='weighted')}
    
    metrics_results = {}
    for metric_name, scorer in scoring_metrics.items():
        scores = cross_val_score(logistic_regression_model, X_features, y_target, cv=5, scoring=scorer)
        avg_score = scores.mean()
        metrics_results[metric_name] = avg_score
    
    # Save the metrics to a CSV file
    save_metrics_to_csv('lr3', 
                        metrics_results['accuracy'], 
                        metrics_results['precision'], 
                        metrics_results['recall'], 
                        metrics_results['f1'])

def svm3(**kwargs):
    heart_disease_df = pd.read_csv("/tmp/heart_disease_merge.csv")
    feature_columns = heart_disease_df.columns.tolist()
    feature_columns.remove('target')
    X_features = heart_disease_df[feature_columns]
    y_target = heart_disease_df['target']
    
    # Splitting the dataset
    X_train_set, X_test_set, y_train_set, y_test_set = train_test_split(X_features, y_target, test_size=0.1, random_state=42)
    
    # Initializing the SVM classifier
    svm_classifier = SVC(random_state=42)
    
    # Training the classifier
    svm_classifier.fit(X_train_set, y_train_set)
    
    # Using cross_val_score for evaluation
    scoring_metrics = {'accuracy': make_scorer(accuracy_score),
                       'precision': make_scorer(precision_score, average='weighted', zero_division=0),
                       'recall': make_scorer(recall_score, average='weighted'),
                       'f1': make_scorer(f1_score, average='weighted')}
    
    metrics_results = {}
    for metric_name, scorer in scoring_metrics.items():
        scores = cross_val_score(svm_classifier, X_features, y_target, cv=5, scoring=scorer)
        avg_score = scores.mean()
        metrics_results[metric_name] = avg_score
    
    # Save the metrics to a CSV file
    save_metrics_to_csv('svm3', 
                        metrics_results['accuracy'], 
                        metrics_results['precision'], 
                        metrics_results['recall'], 
                        metrics_results['f1'])

def compare(**kwargs):
    # Define the paths to the CSV files
    csv_paths = {
        'lr1': '/tmp/lr1_evaluation.csv',
        'lr2': '/tmp/lr2_evaluation.csv',
        'lr3': '/tmp/lr3_evaluation.csv',
        'svm1': '/tmp/svm1_evaluation.csv',
        'svm2': '/tmp/svm2_evaluation.csv',
        'svm3': '/tmp/svm3_evaluation.csv'
    }
    
    # Read accuracies from the CSV files
    accuracies = {}
    for model_name, csv_path in csv_paths.items():
        df = pd.read_csv(csv_path)
        accuracies[model_name] = df['Accuracy'].iloc[0]
    
    # List of accuracies with corresponding model names
    accuracy_list = [(model_name, accuracy) for model_name, accuracy in accuracies.items()]
    print("accuracy list:")
    print(accuracy_list)

    # Find the model name with the highest accuracy
    best_accuracy_model_name, best_accuracy_value = max(accuracy_list, key=lambda x: x[1])

    df_best_model = pd.DataFrame({
        'Best Model': [best_accuracy_model_name],
        'Best Accuracy': [best_accuracy_value]
    })
    out_path = '/tmp/best_model_evaluation.csv'
    # Save the DataFrame to a CSV file
    df_best_model.to_csv(out_path, index=False)

def evaluate(**kwargs):
    # Path to the CSV file containing the best model information
    best_model_csv_path = '/tmp/best_model_evaluation.csv'
    
    # Read the best model information
    df_best_model = pd.read_csv(best_model_csv_path)
    best_model = df_best_model['Best Model'].iloc[0]
    
    # Path to the CSV file containing the metrics for the best model
    best_model_metrics_csv_path = f'/tmp/{best_model}_evaluation.csv'
    
    # Read the metrics for the best model
    df_metrics = pd.read_csv(best_model_metrics_csv_path)
    accuracy = df_metrics['Accuracy'].iloc[0]
    precision = df_metrics['Precision'].iloc[0]
    recall = df_metrics['Recall'].iloc[0]
    f1 = df_metrics['F1 Score'].iloc[0]

    print(f"Best Model: {best_model}\n"
          f"Accuracy: {accuracy:.4f}\n"
          f"Precision: {precision:.4f}\n"
          f"Recall: {recall:.4f}\n"
          f"F1 Score: {f1:.4f}")

    return best_model


def load_data(**kwargs):
    s3 = boto3.client('s3')
    bucket_name = 'de300spring2024'
    key = 'ryan_newkirk/heart_disease.csv'
    local_path = "/tmp/heart_disease.csv"
    s3.download_file(bucket_name, key, local_path)

    key2 = 'ryan_newkirk/homework3/gender_smoke_cdc.csv'
    local_path2 = "/tmp/gender_smoke_cdc.csv"
    s3.download_file(bucket_name, key2, local_path2)

    key3 = 'ryan_newkirk/homework3/age_smoke_cdc.csv'
    local_path3 = "/tmp/age_smoke_cdc.csv"
    s3.download_file(bucket_name, key3, local_path3)

    key4 = 'ryan_newkirk/homework3/smoke_dict_abs.csv'
    local_path4 = "/tmp/smoke_dict_abs.csv"
    s3.download_file(bucket_name, key4, local_path4)

    return local_path

# Instantiate the DAG
dag = DAG(
    'ryan_newkirk_hw4_6',
    default_args=default_args,
    description='Ryan Newkirk HW4',
    schedule_interval=WORKFLOW_SCHEDULE_INTERVAL,
    start_date=datetime(2024, 6, 6),
    tags=["ryannewkirk"]
)

# ==================================================================
# ==================================================================

# Define Airflow Tasks

load_data_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

eda_task = PythonOperator(
    task_id='eda',
    python_callable=eda,
    provide_context=True,
    dag=dag,
)

spark_eda_task = PythonOperator(
    task_id='spark_eda',
    python_callable=spark_eda,
    provide_context=True,
    dag=dag,
)

fe1_task = PythonOperator(
    task_id='feature_engineering_1',
    python_callable=fe1,
    provide_context=True,
    dag=dag,
)

fe2_task = PythonOperator(
    task_id='feature_engineering_2',
    python_callable=fe2,
    provide_context=True,
    dag=dag,
)

merge_task = PythonOperator(
    task_id='merge',
    python_callable=merge,
    provide_context=True,
    dag=dag,
)

lr1_task = PythonOperator(
    task_id='logistic_regression_1',
    python_callable=lr1,
    provide_context=True,
    dag=dag,
)

lr2_task = PythonOperator(
    task_id='logistic_regression_2',
    python_callable=lr2,
    provide_context=True,
    dag=dag,
)

lr3_task = PythonOperator(
    task_id='logistic_regression_3',
    python_callable=lr3,
    provide_context=True,
    dag=dag,
)

svm1_task = PythonOperator(
    task_id='SVM_1',
    python_callable=svm1,
    provide_context=True,
    dag=dag,
)

svm2_task = PythonOperator(
    task_id='SVM_2',
    python_callable=svm2,
    provide_context=True,
    dag=dag,
)

svm3_task = PythonOperator(
    task_id='SVM_3',
    python_callable=svm3,
    provide_context=True,
    dag=dag,
)

compare_task = PythonOperator(
    task_id='compare',
    python_callable=compare,
    provide_context=True,
    dag=dag,
)

evaluate_task = PythonOperator(
    task_id='evaluate',
    python_callable=evaluate,
    provide_context=True,
    dag=dag,
)

remove_smoke1_task = PythonOperator(
    task_id='remove_smoke1',
    python_callable=remove_smoke1,
    provide_context=True,
    dag=dag,
)

remove_smoke2_task = PythonOperator(
    task_id='remove_smoke2',
    python_callable=remove_smoke2,
    provide_context=True,
    dag=dag,
)

# Define the dependencies

#Task 1
load_data_task >> [eda_task, spark_eda_task]

#Task 2
spark_eda_task >> fe1_task
eda_task >> fe2_task

#Task 3
fe1_task >> [merge_task, remove_smoke1_task]
fe2_task >> [merge_task, remove_smoke2_task]

#Task 4
remove_smoke1_task >> [lr1_task, svm1_task]
remove_smoke2_task >> [lr2_task, svm2_task]
merge_task >> [lr3_task, svm3_task]

#Task 5
[lr1_task, svm1_task, lr2_task, svm2_task, lr3_task, svm3_task] >> compare_task

compare_task >> evaluate_task