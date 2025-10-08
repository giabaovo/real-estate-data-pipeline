import scrapy
import json
import logging

class OneHousingAPISpider(scrapy.Spider):
    name = 'onehousing_api'
    
    START_URL = 'https://api.onehousing.vn/onehousing-channel/v1/insights/projects/filter?page=0&size=12' 
    
    DATA_API_URL = 'https://api.onehousing.vn/onehousing-channel/v1/insights/projects/filter'
    
    body = {
        "searchType": "RECOMMENDATION",
        "search_type": "RECOMMENDATION",
        "sort_demand_codes": [
            "CAMPAIGN",
            "PRIMARY_BUYING",
            "SECONDARY_BUYING"
        ]
    }

    def start_requests(self):
        self.logger.info("Bắt đầu crawl: Gọi API để lấy tổng số lượng (total).")
        
        yield scrapy.Request(
            url=self.START_URL,
            method='POST',
            headers={'Content-Type': 'application/json'},
            body=json.dumps(self.body),
            callback=self.parse_total
        )

    def parse_total(self, response):
        """
        Trích xuất meta.total và sử dụng nó để gọi API dữ liệu một lần duy nhất.
        """
        if response.status != 200:
            self.logger.error(f"Yêu cầu lấy Total thất bại. Status: {response.status}")
            return

        try:
            data = response.json()
        except json.JSONDecodeError:
            self.logger.error("Phản hồi không phải là JSON hợp lệ.")
            return

        total_value = None
        
        if 'meta' in data and isinstance(data['meta'], dict) and 'total' in data['meta']:
            total_value = data['meta']['total']
        
        if total_value is not None and total_value > 0:
            self.logger.info(f"Tổng số lượng item được tìm thấy: {total_value}. Chuẩn bị gọi API dữ liệu cuối cùng.")
            
            url_final = f"{self.DATA_API_URL}?page=0&size={total_value}"
            
            yield scrapy.Request(
                url=url_final,
                method='POST',
                headers={'Content-Type': 'application/json'},
                body=json.dumps(self.body),
                callback=self.parse_data
            )
        else:
            self.logger.warning("Không tìm thấy khóa 'meta' hoặc 'total' > 0. Kết thúc crawl.")

    def parse_data(self, response):
        """
        Trích xuất danh sách các đối tượng từ khóa 'data' và yield từng dictionary thô (raw_item).
        """
        if response.status != 200:
            self.logger.error(f"Yêu cầu lấy dữ liệu thất bại. Status: {response.status}")
            return

        try:
            data = response.json()
        except json.JSONDecodeError:
            self.logger.error("Phản hồi dữ liệu cuối cùng không phải là JSON hợp lệ.")
            return
            
        if 'data' in data and isinstance(data['data'], list):
            items_list = data['data']
            self.logger.info(f"Đã lấy thành công {len(items_list)} items.")
            
            for raw_item in items_list:
                yield raw_item
                
        else:
            self.logger.warning("Không tìm thấy khóa 'data' (hoặc không phải là list) trong phản hồi dữ liệu cuối cùng.")
