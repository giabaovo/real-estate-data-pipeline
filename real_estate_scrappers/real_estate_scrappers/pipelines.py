from minio import Minio
from datetime import datetime
import json
import io
import logging
import os

class MinIOLoadPipeline:
    
    def __init__(self):
        # Bộ đệm trong bộ nhớ để lưu JSONL
        self.buffer = io.BytesIO() 
        self.item_count = 0
        self.log = logging.getLogger(__name__)

    @classmethod
    def from_crawler(cls, crawler):
        # Lấy các biến cấu hình từ settings.py
        cls.MINIO_ENDPOINT = crawler.settings.get('MINIO_ENDPOINT')
        cls.MINIO_ACCESS_KEY = crawler.settings.get('MINIO_ACCESS_KEY')
        cls.MINIO_SECRET_KEY = crawler.settings.get('MINIO_SECRET_KEY')
        cls.MINIO_BUCKET = crawler.settings.get('MINIO_BUCKET')

        cls.PROCESS_RUN_ID = os.environ.get('SCRAPY_PROCESS_RUN_ID', 'MANUAL')
        return cls()

    def open_spider(self, spider):
        self.log = spider.logger
        self.crawl_time = datetime.now() # Sử dụng đối tượng datetime để dễ xử lý partition
        self.buffer = io.BytesIO()
        self.item_count = 0

        # Khởi tạo kết nối MinIO khi Spider bắt đầu
        try:
            self.client = Minio(
                self.MINIO_ENDPOINT,
                access_key=self.MINIO_ACCESS_KEY,
                secret_key=self.MINIO_SECRET_KEY,
                secure=False 
            )
            self.log.info(f"Đã kết nối MinIO: {self.MINIO_ENDPOINT}")
        except Exception as e:
            self.log.error(f"Lỗi kết nối MinIO: {e}")
            self.client = None # Vô hiệu hóa client nếu kết nối lỗi

    def process_item(self, item, spider):
        if self.client is None:
            self.log.warning("Không thể xử lý Item do lỗi kết nối MinIO.")
            return item

        # Best Practice: Thêm Metadata vào Bronze Layer
        item['timestamp'] = self.crawl_time.strftime("%Y-%m-%dT%H:%M:%S")
        item['spider_name'] = spider.name
        item['process_run_id'] = self.PROCESS_RUN_ID
        
        # Chuyển Item thành chuỗi JSONL
        try:
            # ensure_ascii=False để giữ tiếng Việt có dấu
            line = json.dumps(item, ensure_ascii=False) + '\n'
            self.buffer.write(line.encode('utf-8'))
            self.item_count += 1
        except Exception as e:
            self.log.error(f"Lỗi khi serialize Item: {e}")
            
        return item

    def close_spider(self, spider):
        if self.client is None:
            self.log.error("Pipeline kết thúc nhưng không thể tải lên MinIO do lỗi kết nối.")
            return
        
        if self.item_count == 0:
            self.log.info("Không có Item nào được thu thập trong lần chạy này.")
            return
            
        self.log.warning(f"Kích hoạt tải file cuối cùng: {self.item_count} Items.")

        # 1. Lấy thông tin Partitioning
        year = self.crawl_time.strftime("%Y")
        month = self.crawl_time.strftime("%m")
        timestamp_str = self.crawl_time.strftime("%Y%m%d_%H%M%S")

        # 2. Định nghĩa tên Object (Best Practice Partitioning)
        # s3://<BUCKET>/bronze/<spider_name>/year=<YYYY>/month=<MM>/<timestamp>.jsonl
        object_prefix = f"bronze/{spider.name}/year={year}/month={month}"
        # Tên file không cần index vì chỉ có 1 chunk
        object_name = f"{object_prefix}/{timestamp_str}.jsonl"
            
        try:
            # Đảm bảo bucket tồn tại trước khi tải
            if not self.client.bucket_exists(self.MINIO_BUCKET):
                self.client.make_bucket(self.MINIO_BUCKET)
                self.log.info(f"Đã tạo bucket MinIO: {self.MINIO_BUCKET}")
                
            # Đặt con trỏ về đầu buffer và tải lên toàn bộ dữ liệu
            self.buffer.seek(0)
            
            self.client.put_object(
                self.MINIO_BUCKET,
                object_name,
                self.buffer,
                self.buffer.getbuffer().nbytes,
                content_type='application/jsonl'
            )
            self.log.warning(
                f"TẢI THÀNH CÔNG: {self.item_count} Items | Kích thước: {self.buffer.getbuffer().nbytes / (1024 * 1024):.2f} MB"
            )
            self.log.info(f"Đường dẫn MinIO: {self.MINIO_BUCKET}/{object_name}")

        except Exception as e:
            self.log.error(f"LỖI TẢI FILE: Không thể tải file '{object_name}' lên MinIO. Lỗi: {e}")
            
        finally:
            # Giải phóng tài nguyên sau khi Spider kết thúc
            self.buffer.close()
            self.log.info("MinIO Pipeline đã đóng. Tài nguyên đã được giải phóng.")
