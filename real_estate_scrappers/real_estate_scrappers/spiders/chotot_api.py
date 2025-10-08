import scrapy
import json
import math
from urllib.parse import urlencode

class ChototProjectApiSpider(scrapy.Spider):
    name = 'chotot_api'

    custom_settings = {
        # Bật Autothrottle để tự điều chỉnh tốc độ, chống bị chặn
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_START_DELAY': 1.0,     # Độ trễ ban đầu
        'AUTOTHROTTLE_MAX_DELAY': 10.0,      # Độ trễ tối đa
        
        # Đảm bảo có độ trễ tối thiểu (nếu Autothrottle bị tắt)
        'DOWNLOAD_DELAY': 0.5, 
        
        # Giảm số lượng request đồng thời để an toàn hơn khi Autothrottle chạy
        'CONCURRENT_REQUESTS': 8,
        
        # Nếu muốn cực kỳ an toàn, chỉ cho phép 1 request đồng thời trên 1 domain
        # 'CONCURRENT_REQUESTS_PER_DOMAIN': 1, 
    }
    # =================================================================
    
    # URL cơ sở và các tham số Paging
    BASE_URL = 'https://gateway.chotot.com/v1/public/api-pty/project'
    ITEM_LIMIT = 100 # Đặt limit là 100 để giảm số lần request
    
    # Headers cần thiết
    DEFAULT_HEADERS = {
        'Content-Type': 'application/json',
        # Thêm User-Agent để mô phỏng trình duyệt, giúp tránh bị chặn
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
    }
    
    def start_requests(self):
        """
        Khởi tạo request đầu tiên với offset=0 để lấy tổng số 'total'.
        """
        params = {'limit': self.ITEM_LIMIT, 'offset': 0}
        first_page_url = f"{self.BASE_URL}?{urlencode(params)}"
        
        self.logger.info(f"Khởi tạo request: {first_page_url}")

        yield scrapy.Request(
            url=first_page_url,
            method='GET',
            headers=self.DEFAULT_HEADERS,
            callback=self.parse_first_response,
        )

    def parse_first_response(self, response):
        """
        Xử lý phản hồi đầu tiên: Trích xuất Item, lấy tổng số 'total' và lên lịch cho toàn bộ quá trình crawl.
        """
        try:
            data = json.loads(response.text)
        except json.JSONDecodeError:
            self.logger.error(f"Lỗi JSON Decode từ URL: {response.url}")
            return

        total_items = data.get('total')
        
        if total_items is None:
            self.logger.error("Không tìm thấy trường 'total' trong response. Dừng crawl.")
            return

        self.logger.info(f"Tổng số Items cần crawl: {total_items}")
        
        # 1. Trích xuất Items của trang đầu tiên (trang 0)
        items = data.get('projects', [])
        for item in items:
            yield item

        # 2. Tính toán và lên lịch các Request còn lại
        
        # Nếu tổng số items lớn hơn limit của trang đầu tiên
        if total_items > self.ITEM_LIMIT:
            # Bắt đầu từ offset = self.ITEM_LIMIT (bỏ qua trang đầu tiên đã crawl)
            current_offset = self.ITEM_LIMIT 
            
            # Tiếp tục cho đến khi offset vượt quá hoặc bằng total_items
            while current_offset < total_items:
                next_params = {'limit': self.ITEM_LIMIT, 'offset': current_offset}
                next_url = f"{self.BASE_URL}?{urlencode(next_params)}"
                
                self.logger.debug(f"Lên lịch crawl offset: {current_offset}/{total_items}")
                
                yield scrapy.Request(
                    url=next_url,
                    method='GET',
                    headers=self.DEFAULT_HEADERS,
                    callback=self.parse_subsequent_response,
                )
                
                # Tăng offset lên bằng limit cho lần gọi tiếp theo
                current_offset += self.ITEM_LIMIT
        
        else:
            self.logger.info("Hoàn tất crawl: Đã lấy tất cả Items trong lần gọi đầu tiên.")


    def parse_subsequent_response(self, response):
        """
        Xử lý phản hồi từ các Request tiếp theo (chỉ trích xuất Item).
        """
        try:
            data = json.loads(response.text)
        except json.JSONDecodeError:
            self.logger.error(f"Lỗi JSON Decode từ URL: {response.url}")
            return

        items = data.get('projects', [])
        
        # Lặp qua từng item và đẩy vào Pipeline chung (MinIOLoadPipeline)
        for item in items:
            yield item
