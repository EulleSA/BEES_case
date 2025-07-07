from pyspark.sql import DataFrame
from pyspark.sql.functions import count
import logging
from utils.configs import ConfigJob
class SilverToGold(ConfigJob):

    def __init__(self, source_path: str, destination_path: str, partition_col: str):
        """
        Initialize the SilverToGold pipeline with the silver and gold paths and the partition column.
        """
        super().__init__(source_path, destination_path, partition_col)


    def _load_data(self) -> DataFrame:
        """"
        Load the DataFrame from the silver path in Delta format."""
        logging.info(f"Loading data from {self.source_path}")
        df = self.spark.read.format("delta").load(f"s3a://{self.source_path}")
        return df
    
    def save_data(self, df: DataFrame) -> DataFrame:
        """"
        Save the DataFrame to the gold path in Delta format, partitioned by the specified column.
        """
        logging.info(f"Saving data to {self.destination_path} with partitioning by {self.partition_col}")
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy(self.partition_col) \
            .option("path", f"s3a://{self.destination_path}") \
            .save()
    
    def _aggregations(self, df: DataFrame) -> DataFrame:
        """"
        Perform aggregations on the DataFrame to count the number of breweries by country and type."""
        df_aggregated = df.groupBy("country", "brewery_type").agg(count("*").alias("total_breweries"))
        return df_aggregated


if __name__ == "__main__":
    pipeline = SilverToGold(f"silver/", f"gold/", "country")
    df = pipeline._load_data()
    df_aggregated = pipeline._aggregations(df)
    pipeline.save_data(df_aggregated)