from minio import Minio
from datetime import datetime
import json
import io
import logging
import os

class MinIOLoadPipeline:
    def __init__(self):
        self.buffer = io.BytesIO() 
        self.item_count = 0
        self.log = logging.getLogger(__name__)

    @classmethod
    def from_crawler(cls, crawler):
        cls.MINIO_ENDPOINT = crawler.settings.get('MINIO_ENDPOINT')
        cls.MINIO_ACCESS_KEY = crawler.settings.get('MINIO_ACCESS_KEY')
        cls.MINIO_SECRET_KEY = crawler.settings.get('MINIO_SECRET_KEY')
        cls.MINIO_BUCKET = crawler.settings.get('MINIO_BUCKET')

        cls.PROCESS_RUN_ID = os.environ.get('SCRAPY_PROCESS_RUN_ID', 'MANUAL')
        return cls()

    def open_spider(self, spider):
        self.log = spider.logger
        self.crawl_time = datetime.now()
        self.buffer = io.BytesIO()
        self.item_count = 0

        try:
            self.client = Minio(
                self.MINIO_ENDPOINT,
                access_key=self.MINIO_ACCESS_KEY,
                secret_key=self.MINIO_SECRET_KEY,
                secure=False 
            )
            self.log.info(f"Connected to MinIO: {self.MINIO_ENDPOINT}")
        except Exception as e:
            self.log.error(f"MinIO connection error: {e}")
            self.client = None

    def process_item(self, item, spider):
        if self.client is None:
            self.log.warning("Cannot process Item due to MinIO connection error.")
            return item

        item['timestamp'] = self.crawl_time.strftime("%Y-%m-%dT%H:%M:%S")
        item['spider_name'] = spider.name
        item['process_run_id'] = self.PROCESS_RUN_ID
        
        try:
            line = json.dumps(item, ensure_ascii=False) + '\n'
            self.buffer.write(line.encode('utf-8'))
            self.item_count += 1
        except Exception as e:
            self.log.error(f"Error when serialize Item: {e}")
            
        return item

    def close_spider(self, spider):
        if self.client is None:
            self.log.error("Pipeline finished but cannot upload to MinIO due to connection error.")
            return
        
        if self.item_count == 0:
            self.log.info("No Items were collected in this run.")
            return

        year = self.crawl_time.strftime("%Y")
        month = self.crawl_time.strftime("%m")
        timestamp_str = self.crawl_time.strftime("%Y%m%d_%H%M%S")

        object_prefix = f"bronze/{spider.name}/year={year}/month={month}"

        object_name = f"{object_prefix}/{timestamp_str}.jsonl"
            
        try:
            if not self.client.bucket_exists(self.MINIO_BUCKET):
                self.client.make_bucket(self.MINIO_BUCKET)
                self.log.info(f"Bucket MinIO created: {self.MINIO_BUCKET}")
                
            self.buffer.seek(0)
            
            self.client.put_object(
                self.MINIO_BUCKET,
                object_name,
                self.buffer,
                self.buffer.getbuffer().nbytes,
                content_type='application/jsonl'
            )
            self.log.warning(
                f"Successfully upload: {self.item_count} Items | Size: {self.buffer.getbuffer().nbytes / (1024 * 1024):.2f} MB"
            )
            self.log.info(f"MinIO Path: {self.MINIO_BUCKET}/{object_name}")

        except Exception as e:
            self.log.error(f"FILE UPLOAD ERROR: Can not upload file '{object_name}' to MinIO. Error: {e}")
            
        finally:
            self.buffer.close()
            self.log.info("MinIO Pipeline has closed.")
