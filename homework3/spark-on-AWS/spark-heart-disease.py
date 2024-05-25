from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, mean, lit, udf
from pyspark.sql.types import IntegerType, FloatType, StringType, StructType, StructField
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier, LogisticRegression, DecisionTreeClassifier, GBTClassifier
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql import Row
import random

if __name__ == "__main__":
    # Create Spark session
    spark = SparkSession \
        .builder \
        .appName("randomSample") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")

    random_seed = 42
    random.seed(random_seed)
    
    # Read data
    df = spark.read.csv("s3://de300spring2024/ryan_newkirk/heart_disease.csv", header=True, inferSchema=True)
    
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

    # PART 2: Perform Data Imputation for the Smoke column:
    # Define the schema for the DataFrames
    gender_schema = StructType([
        StructField("gender", StringType(), True),
        StructField("percentage", FloatType(), True)
    ])

    age_cdc_schema = StructType([
        StructField("age_range", StringType(), True),
        StructField("percentage", FloatType(), True)
    ])

    smoke_abs_schema = StructType([
        StructField("age_range", StringType(), True),
        StructField("percentage", FloatType(), True)
    ])

    # Read the data from CSV into Spark DataFrames
    df_gender_smoke_cdc = spark.read.csv("s3://de300spring2024/ryan_newkirk/homework3/gender_smoke_cdc.csv", header=True, schema=gender_schema)
    df_age_smoke_cdc = spark.read.csv("s3://de300spring2024/ryan_newkirk/homework3/age_smoke_cdc.csv", header=True, schema=age_cdc_schema)
    df_smoke_dict_abs = spark.read.csv("s3://de300spring2024/ryan_newkirk/homework3/smoke_dict_abs.csv", header=True, schema=smoke_abs_schema)

    # Convert data to dictionaries for easier access
    gender_smoke_dict = {row.gender: row.percentage for row in df_gender_smoke_cdc.collect()}
    age_smoke_dict = {row.age_range: row.percentage for row in df_age_smoke_cdc.collect()}
    smoke_abs_dict = {row.age_range: row.percentage for row in df_smoke_dict_abs.collect()}

    # print("Gender Smoke Dictionary:", gender_smoke_dict)
    # print("Age Smoke Dictionary:", age_smoke_dict)
    # print("Smoke ABS Dictionary:", smoke_abs_dict)

    # Define a UDF to impute the 'smoke' column
    @udf(returnType=IntegerType())
    def impute_smoke(sex, age):
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

    # Apply the impute_smoke UDF to the DataFrame to fill missing 'smoke' values
    # df_step1 = df_step1.withColumn('smoke_imputed', when(col('smoke').isNull(), impute_smoke(col('sex'), col('age'))).otherwise(col('smoke')))
    df = df_step1

    # 1. Initial Check
    df.select('smoke').show(10)

    # Count missing values in 'smoke' before imputation
    missing_count_before = df.filter(col('smoke').isNull()).count()
    
    # 2. Apply the Imputation
    df_step1 = df.withColumn('smoke_imputed', when(col('smoke').isNull(), impute_smoke(col('sex'), col('age'))).otherwise(col('smoke')))

    # 3. Check After Imputation
    df_step1.select('smoke', 'smoke_imputed').show(10)

    # Count missing values in 'smoke_imputed' after imputation
    missing_count_after = df_step1.filter(col('smoke_imputed').isNull()).count()
    
    # 4. Finalize the Imputation (Optional based on verification)
    # If the imputation is verified to be correct, you might replace 'smoke' with 'smoke_imputed'
    df_final = df_step1.drop('smoke').withColumnRenamed('smoke_imputed', 'smoke')

    # Final check
    df_final.select('smoke').show(10)

    df_step1 = df_final

    # Final Part: performing Tasks 3-5

    # Assuming the last column is the target, and the rest are features
    feature_cols = [col for col in df_step1.columns if col != 'target']
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    df_assembled = assembler.transform(df_step1)

    # Scale the features
    scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures", withStd=True, withMean=False)

    # Split the data into training and test sets
    train, test = df_assembled.randomSplit([0.9, 0.1], seed=42)

    # Define models
    models = {
        "Random Forest": RandomForestClassifier(featuresCol="scaledFeatures", labelCol="target"),
        "Logistic Regression": LogisticRegression(featuresCol="scaledFeatures", labelCol="target", maxIter=2000),
        "Decision Tree": DecisionTreeClassifier(featuresCol="scaledFeatures", labelCol="target"),
        "Gradient Boosting": GBTClassifier(featuresCol="scaledFeatures", labelCol="target"),
    }

    # Define evaluators
    evaluators = {
        "accuracy": MulticlassClassificationEvaluator(labelCol="target", metricName="accuracy"),
        "precision": MulticlassClassificationEvaluator(labelCol="target", metricName="weightedPrecision"),
        "recall": MulticlassClassificationEvaluator(labelCol="target", metricName="weightedRecall"),
        "f1": MulticlassClassificationEvaluator(labelCol="target", metricName="f1")
    }

    results_text = ""

    for name, model in models.items():
        results_text += f"Model: {name}\n"
        # Define pipeline
        pipeline = Pipeline(stages=[scaler, model])

        # Define parameter grid
        if name == "Random Forest":
            paramGrid = ParamGridBuilder() \
                .addGrid(model.maxDepth, [5, 10]) \
                .addGrid(model.numTrees, [20, 50]) \
                .build()
        elif name == "Logistic Regression":
            paramGrid = ParamGridBuilder() \
                .addGrid(model.regParam, [0.01, 0.1]) \
                .addGrid(model.elasticNetParam, [0.0, 0.5, 1.0]) \
                .build()
        elif name == "Decision Tree":
            paramGrid = ParamGridBuilder() \
                .addGrid(model.maxDepth, [5, 10]) \
                .build()
        elif name == "Gradient Boosting":
            paramGrid = ParamGridBuilder() \
                .addGrid(model.maxDepth, [5, 10]) \
                .addGrid(model.maxIter, [10, 20]) \
                .build()
        else:
            paramGrid = ParamGridBuilder().build()  # Empty grid for models with no tuning

        # Define cross-validator
        cv = CrossValidator(estimator=pipeline,
                            estimatorParamMaps=paramGrid,
                            evaluator=evaluators["f1"],  # Use F1 for model selection
                            numFolds=5)

        # Fit the model
        cvModel = cv.fit(train)

        # Predict and evaluate
        predictions = cvModel.transform(test)

        # Evaluate each metric
        for metric_name, evaluator in evaluators.items():
            metric_value = evaluator.evaluate(predictions)
            results_text += f"{metric_name.capitalize()}: {metric_value:.3f}\n"

        results_text += "\n"

    # Convert the results text to a Spark DataFrame
    results_df = spark.createDataFrame([Row(result=results_text)])

    # Save the resulting DataFrame to a new text file in the S3 bucket
    results_df.write.mode('overwrite').text("s3://de300spring2024/ryan_newkirk/homework3/heart_disease_report.txt")

    spark.sparkContext.stop()