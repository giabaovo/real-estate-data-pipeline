import scrapy
import json

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
        self.logger.info("Start crawling.")
        
        yield scrapy.Request(
            url=self.START_URL,
            method='POST',
            headers={'Content-Type': 'application/json'},
            body=json.dumps(self.body),
            callback=self.parse_total
        )

    def parse_total(self, response):
        if response.status != 200:
            self.logger.error(f"Request to retrieve Total failed. Status: {response.status}")
            return

        try:
            data = response.json()
        except json.JSONDecodeError:
            self.logger.error("Invalid JSON.")
            return

        total_value = None
        
        if 'meta' in data and isinstance(data['meta'], dict) and 'total' in data['meta']:
            total_value = data['meta']['total']
        
        if total_value is not None and total_value > 0:
            self.logger.info(f"Total items found: {total_value}.")
            
            url_final = f"{self.DATA_API_URL}?page=0&size={total_value}"
            
            yield scrapy.Request(
                url=url_final,
                method='POST',
                headers={'Content-Type': 'application/json'},
                body=json.dumps(self.body),
                callback=self.parse_data
            )
        else:
            self.logger.warning("Key 'meta' or 'total' not found > 0. Ending crawl.")

    def parse_data(self, response):
        if response.status != 200:
            self.logger.error(f"Request to retrieve data failed. Status: {response.status}")
            return

        try:
            data = response.json()
        except json.JSONDecodeError:
            self.logger.error("Invalid JSON.")
            return
            
        if 'data' in data and isinstance(data['data'], list):
            items_list = data['data']
            self.logger.info(f"Successfully get {len(items_list)} items.")
            
            for raw_item in items_list:
                yield raw_item
                
        else:
            self.logger.warning("Key 'data' not found.")
