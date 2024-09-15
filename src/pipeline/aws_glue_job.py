import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
import pyspark.sql.types as T

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

logger = glueContext.get_logger()
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


def flatten_schema(schema: T.StructType, prefix: str = "") -> list:
    """
    Flattens a Spark DataFrame schema into a list of column paths.
    Args:
        schema (StructType): The schema to flatten.
        prefix (str): Prefix for nested column paths.

    Returns:
        list: List of column paths as strings.
    """
    fields = []
    for field in schema.fields:
        name = f"{prefix}{field.name}"
        if isinstance(field.dataType, T.StructType):
            fields.extend(flatten_schema(field.dataType, f"{name}."))
        else:
            fields.append(name)
    return fields


def clean_json_columns(df: DataFrame, columns: list) -> DataFrame:
    """
    Cleans JSON columns by standardizing boolean values and removing special characters.
    Args:
        df (DataFrame): DataFrame to process.
        columns (list): List of JSON column names to clean.
    Returns:
        DataFrame: DataFrame with cleaned columns.
    """
    for col in columns:
        new_col_name = col.replace(".", "_").lower()
        df = df.withColumn(new_col_name, F.regexp_replace(F.col(col), "^u?'|'?$", ""))
        df = df.withColumn(
            new_col_name, F.regexp_replace(F.col(new_col_name), "True", "true")
        )
        df = df.withColumn(
            new_col_name, F.regexp_replace(F.col(new_col_name), "False", "false")
        )
        df = df.withColumn(
            new_col_name,
            F.when(F.lower(F.col(new_col_name)) == "none", None).otherwise(
                F.col(new_col_name)
            ),
        )
    df = df.drop("attributes")
    return df


def get_json_string_columns(df: DataFrame, columns: list) -> list:
    """
    Identifies columns that contain JSON-like strings.
    Args:
        df (DataFrame): DataFrame to inspect.
        columns (list): List of column names to check.
    Returns:
        list: List of column names containing JSON strings.
    """
    json_columns = []
    for col_name in columns:
        json_like_objects = (
            df.filter(F.col(col_name).startswith("{") & F.col(col_name).endswith("}"))
            .limit(1)
            .collect()
        )
        if json_like_objects:
            json_columns.append(col_name)
    return json_columns


def convert_string_columns_to_json(df: DataFrame, json_columns: list) -> DataFrame:
    """
    Converts string columns that contain JSON into structured JSON columns.
    Args:
        df (DataFrame): DataFrame with string columns.
        json_columns (list): List of column names to convert.
    Returns:
        DataFrame: DataFrame with JSON columns.
    """
    for col_name in json_columns:
        valid_json_df = df.filter(
            F.col(col_name).isNotNull() & F.col(col_name).substr(1, 1).eqNullSafe("{")
        )
        sample_json = valid_json_df.limit(1).collect()

        if sample_json:
            # Infer schema from the first valid JSON
            inferred_schema = F.schema_of_json(F.lit(sample_json[0][col_name]))
            df = df.withColumn(col_name, F.from_json(F.col(col_name), inferred_schema))
        else:
            # If no valid JSON found,set up default or empty schema
            df = df.withColumn(col_name, F.lit(None).cast("string"))

    return df


def clean_hour_columns(df: DataFrame, columns: list) -> DataFrame:
    """
    Splits hour columns into separate opening and closing time columns.
    Args:
        df (DataFrame): DataFrame containing hour information.
        columns (list): List of hour column names.
    Returns:
        DataFrame: DataFrame with processed hour columns.
    """
    for col in columns:
        df = df.withColumn(
            f"{col.split('.')[-1].lower()}_opening_time",
            F.split(df[col], "-").getItem(0),
        )
        df = df.withColumn(
            f"{col.split('.')[-1].lower()}_closing_time",
            F.split(df[col], "-").getItem(1),
        )
    df = df.drop("hours")
    return df


def process_business_attributes(df: DataFrame) -> DataFrame:
    """
    Processes and cleans business attribute columns in a DataFrame.
    Args:
        df (DataFrame): DataFrame to process.
    Returns:
        DataFrame: DataFrame with processed attributes.
    """
    # This code is a bit redundant and could be re-written
    # for better clarity and optimization
    flattened_schema = flatten_schema(df.schema)
    attributes_cols = [f for f in flattened_schema if f.startswith("attributes")]
    df = clean_json_columns(df, attributes_cols)
    attributes_cols_new = [col for col in df.columns if col.startswith("attributes")]
    json_columns = get_json_string_columns(df, attributes_cols_new)
    df = convert_string_columns_to_json(df, json_columns)
    flattened_schema_v2 = flatten_schema(df.select(json_columns).schema)
    attributes_cols_nested = [
        f for f in flattened_schema_v2 if f.startswith("attributes")
    ]
    df = clean_json_columns(df, attributes_cols_nested)
    df = df.drop(*json_columns)
    return df


def process_business_hours(df: DataFrame) -> DataFrame:
    """
    Processes and cleans business hours columns in a DataFrame.
    Args:
        df (DataFrame): DataFrame to process.
    Returns:
        DataFrame: DataFrame with processed hours.
    """
    flattened_schema = flatten_schema(df.schema)
    hours_cols = [f for f in flattened_schema if f.startswith("hours")]
    df = clean_hour_columns(df, hours_cols)
    return df


def process_business(df: DataFrame) -> DataFrame:
    """
    Processes business data by cleaning attributes and transforming columns.
    Args:
        df (DataFrame): DataFrame containing business data.
    Returns:
        DataFrame: DataFrame with processed business data.
    """
    df = df.withColumn("is_open", F.col("is_open").cast(T.BooleanType()))
    df = process_business_hours(df)
    df = process_business_attributes(df)
    return df


def process_checkins(df: DataFrame) -> DataFrame:
    """
    Processes check-in data by flattening the date columns.
    Args:
        df (DataFrame): DataFrame containing check-in data.
    Returns:
        DataFrame: DataFrame with processed check-in data.
    """
    df = df.withColumn("date", F.split(F.col("date"), ","))
    df = df.select(df.business_id, F.explode(df.date).alias("date"))
    df = df.withColumn("date", F.to_timestamp("date"))
    return df


def process_users(df: DataFrame) -> DataFrame:
    """
    Processes user data by cleaning and transforming relevant columns.
    Args:
        df (DataFrame): DataFrame containing user data.
    Returns:
        DataFrame: DataFrame with processed user data.
    """
    df = df.withColumn(
        "elite_years",
        F.when(F.col("elite") == "", None).otherwise(F.split("elite", ",")),
    ).drop("elite")
    df = df.withColumn(
        "friends",
        F.when(F.col("friends") == "", None).otherwise(F.split("friends", ",")),
    )
    df = df.withColumn("yelping_since", F.to_timestamp("yelping_since"))
    df = df.withColumn(
        "friends_count",
        F.when(F.col("friends").isNull(), 0).otherwise(F.size(df["friends"])),
    ).drop("friends")
    df = df.withColumn(
        "elite_years_count",
        F.when(F.col("elite_years").isNull(), 0).otherwise(F.size(df["elite_years"])),
    ).drop("elite_years")
    return df


def process_reviews_and_tips(df: DataFrame) -> DataFrame:
    """
    Processes review and tip data by enriching date columns.
    Args:
        df (DataFrame): DataFrame to process.
    Returns:
        DataFrame: DataFrame with enriched date data.
    """
    df = df.withColumn("date", F.to_timestamp("date"))
    return df


def read_dynamic_and_convert_df(
    glueContext: GlueContext, database: str, table_name: str
) -> DataFrame:
    try:
        dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
            database=database, table_name=table_name
        )
        logger.info(f"Successfully read data from {table_name} in {database}")
        return dynamic_frame.toDF()
    except Exception as e:
        logger.error(
            f"Failed to read from table {table_name} in database {database}: {str(e)}"
        )
        return None


def process_all_and_write(
    glueContext: GlueContext, df: DataFrame, process_function, output_path: str
) -> None:
    try:
        processed_df = process_function(df)
        # Expensive operation, probably not optimal
        processed_df = processed_df.coalesce(1)
        if processed_df and processed_df.count() > 0:
            processed_dyf = DynamicFrame.fromDF(
                processed_df, glueContext, "processed_dyf"
            )
            glueContext.write_dynamic_frame.from_options(
                frame=processed_dyf,
                connection_type="s3",
                connection_options={"path": output_path},
                format="parquet",
                format_options={"compression": "snappy"},
            )
            logger.info(f"Data successfully written to {output_path}")
    except Exception as e:
        logger.error(
            f"Error processing or writing data for output path {output_path}: {str(e)}"
        )


common_output_path = "s3://yelp-project-jp/flattened_data/"
database = "yelp"
tables_and_functions = {
    "yelp_business": (process_business, common_output_path),
    "yelp_checkin": (process_checkins, common_output_path),
    "yelp_user": (process_users, common_output_path),
    "yelp_review": (process_reviews_and_tips, common_output_path),
    "yelp_tip": (process_reviews_and_tips, common_output_path),
}
for table_name, (process_function, output_path) in tables_and_functions.items():
    logger.info(f"Starting processing of {table_name}")
    df = read_dynamic_and_convert_df(
        glueContext=glueContext, database=database, table_name=table_name
    )
    process_all_and_write(
        glueContext=glueContext,
        df=df,
        process_function=process_function,
        output_path=output_path,
    )

job.commit()
