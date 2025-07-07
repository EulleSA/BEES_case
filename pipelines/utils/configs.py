from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
class ConfigJob:
    def __init__(self, source_path: str, destination_path: str, partition_col: str = None):
        self.spark = SparkSession.builder \
        .appName("BronzeToSilver") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2,io.delta:delta-spark_2.12:3.1.0") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .getOrCreate()
        self.source_path = source_path
        self.destination_path = destination_path
        self.partition_col = "country" if partition_col is None else partition_col
    
    def load_data(self) -> DataFrame:
        """Load the DataFrame from the source path in Delta format."""
        return self.spark.read.format("delta").load(f"s3a://{self.source_path}")
    def save_data(self, df) -> None:
        """Save the DataFrame to the destination path in Delta format, partitioned by the specified column."""
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy(self.partition_col) \
            .option("path", f"s3a://{self.destination_path}") \
            .save()