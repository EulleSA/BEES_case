from datetime import datetime
import logging
logging.basicConfig(level=logging.INFO)
from utils.configs import ConfigJob
class BronzeToSilver(ConfigJob):

    def __init__(self, source_path: str, destination_path: str, partition_col: str):
        super().__init__(source_path, destination_path, partition_col)

    def load_data(self):
        """"Load the DataFrame from the bronze path in JSON format."""
        logging.info(f"Loading data from {self.source_path}")
        return self.spark.read.option("multiLine","true").format("json").load(f"s3a://{self.source_path}")

    def save_data(self, df):
        """"Save the DataFrame to the silver path in Delta format, partitioned by the specified column."""
        logging.info(f"Saving data to {self.destination_path} with partitioning by {self.partition_col}")
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy(self.partition_col) \
            .option("path", f"s3a://{self.destination_path}") \
            .save()

if __name__ == "__main__":

    partition_col = "country"
    silver_path = "silver/"
    bronze_path = f"bronze/data_{datetime.now().strftime('%Y-%m-%d')}.json"
    pipeline = BronzeToSilver(
        bronze_path,
        silver_path,
        partition_col
    )
    df = pipeline.load_data()
    pipeline.save_data(df)
    print(f"Data saved to {silver_path} in Delta format with partitioning by {partition_col}.")