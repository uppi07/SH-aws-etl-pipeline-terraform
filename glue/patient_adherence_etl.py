import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import col, lower, trim, when

args = getResolvedOptions(sys.argv, ["JOB_NAME", "input_path", "output_path"])

# Set up Glue and Spark contexts for this job run.
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

input_path = args["input_path"]
output_path = args["output_path"]

try:
    # Load the raw CSV from S3.
    print("Starting ETL job")
    df = spark.read.option("header", "true").csv(input_path)
    print("Dataset loaded successfully")
    rows_read = df.count()
    print("Rows read:", rows_read)
    required_columns = [
        "Age",
        "Gender",
        "Medication_Type",
        "Dosage_mg",
        "Condition_Severity",
        "Comorbidities_Count",
        "Adherence"
    ]

    for column in required_columns:
        if column not in df.columns:
            raise ValueError(f"Missing required column: {column}")

    print("Schema validation passed")

    # Clean and type the core fields first so downstream risk logic is reliable.
    numeric_columns = {
        "Age": "int",
        "Dosage_mg": "double",
        "Comorbidities_Count": "int",
        "Adherence": "int",
    }

    for column_name, data_type in numeric_columns.items():
        df = df.withColumn(column_name, col(column_name).cast(data_type))

    print("Schema after casting:")
    df.printSchema()

    df = df.dropna(subset=["Age", "Medication_Type", "Adherence", "Comorbidities_Count"])
    rows_after_cleaning = df.count()
    rows_dropped = rows_read - rows_after_cleaning
    print("Rows dropped:", rows_dropped)

    # Normalize text columns to keep category values consistent.
    df = df.withColumn("Gender", lower(trim(col("Gender"))))
    df = df.withColumn("Medication_Type", lower(trim(col("Medication_Type"))))
    df = df.withColumn("Condition_Severity", lower(trim(col("Condition_Severity"))))

    # Create a simple derived feature for downstream analysis.
    df = df.withColumn(
        "risk_level",
        when(col("Comorbidities_Count") >= 3, "high_risk")
        .otherwise("normal_risk")
    )

    print("Transformation completed")
    df = df.select(
        "Age",
        "Gender",
        "Medication_Type",
        "Dosage_mg",
        "Condition_Severity",
        "Comorbidities_Count",
        "Adherence",
        "risk_level"
    )

    # Write the transformed dataset back to S3 in Parquet format.
    print("Selected analytics-ready columns")
    df.write.mode("overwrite").parquet(output_path)
    rows_written = rows_after_cleaning
    print("Rows written:", rows_written)
    print("Processed data written to:", output_path)
    print("ETL job completed successfully")
    job.commit()
    
except Exception as e:
    # Log the error and fail the job so Glue marks this run as failed.
    print("ETL job failed:", str(e))
    raise
