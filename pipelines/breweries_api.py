import requests
import time
import logging
import boto3
import json
logging.basicConfig(level=logging.INFO)
from datetime import datetime

class CollectData:

    def __init__(self, endpoint, page):
        self.endpoint = endpoint
        self.per_page = 200
        self.page = page
        self.__minio_endpoint = "http://minio:9000"
        self.__minio_access_key = "minioadmin"
        self.__minio_secret_key = "minioadmin"
        self.bucket_name = "bronze"
        self.file_path = f"/datalake/bronze"
        self.object_name = f"data_{datetime.now().strftime('%Y-%m-%d')}.json"

    def list_endpoint(self):
        list_data = []
        finished = False
        while not finished:
            response_api = requests.get(self.endpoint, params={"per_page": self.per_page, "page":self.page},timeout=5)
            try:
                if response_api.status_code == 200:
                    if response_api.json():
                        logging.info(f"Obtendo dados da página {self.page}")
                        list_data.extend(response_api.json())
                        self.page += 1
                    else:
                        logging.info("Todas as páginas percorridas")
                        finished = True
                else:
                    logging.error("Erro ao obter os dados")
                    time.sleep(10)
            except Exception as e:
                logging.error(f"{e}")

        return list_data
    
    def _check_page_exists(self):
        pass
    
    def save_data(self):
        
        s3_client = boto3.client(
            's3',
            endpoint_url=self.__minio_endpoint,
            aws_access_key_id=self.__minio_access_key,
            aws_secret_access_key=self.__minio_secret_key,
        )

        try:
            s3_client.head_bucket(Bucket=self.bucket_name)
        except:
            s3_client.create_bucket(Bucket=self.bucket_name)
            
        json_data = json.dumps(self.list_endpoint(), indent=4)
        s3_client.put_object(Bucket=self.bucket_name, Key=self.object_name, Body=json_data, ContentType='application/json')
        logging.info(f"Arquivo {self.file_path} enviado para o bucket {self.bucket_name} como {self.object_name}.")



if __name__ == "__main__":

    final_data = CollectData("https://api.openbrewerydb.org/v1/breweries", 1)
    final_data.save_data()
