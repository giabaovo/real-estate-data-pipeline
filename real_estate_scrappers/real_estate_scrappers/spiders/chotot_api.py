import scrapy
import json
from urllib.parse import urlencode

class ChototProjectApiSpider(scrapy.Spider):
    name = 'chotot_api'

    custom_settings = {
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_START_DELAY': 1.0,
        'AUTOTHROTTLE_MAX_DELAY': 10.0,
        'DOWNLOAD_DELAY': 0.5, 
        'CONCURRENT_REQUESTS': 8,
    }

    BASE_URL = 'https://gateway.chotot.com/v1/public/api-pty/project'
    ITEM_LIMIT = 100

    DEFAULT_HEADERS = {
        'Content-Type': 'application/json',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
    }
    
    def start_requests(self):
        """
        Initialize the first request with offset=0 to retrieve the 'total' count.
        """
        params = {'limit': self.ITEM_LIMIT, 'offset': 0}
        first_page_url = f"{self.BASE_URL}?{urlencode(params)}"
        
        self.logger.info(f"Initial request: {first_page_url}")

        yield scrapy.Request(
            url=first_page_url,
            method='GET',
            headers=self.DEFAULT_HEADERS,
            callback=self.parse_first_response,
        )

    def parse_first_response(self, response):
        try:
            data = json.loads(response.text)
        except json.JSONDecodeError:
            self.logger.error(f"JSON Decode error from URL: {response.url}")
            return

        total_items = data.get('total')
        
        if total_items is None:
            self.logger.error("Field 'total' not found in response. Stopping crawl.")
            return

        self.logger.info(f"Total items need to crawl: {total_items}")
        
        items = data.get('projects', [])
        for item in items:
            yield item

        if total_items > self.ITEM_LIMIT:
            current_offset = self.ITEM_LIMIT 

            while current_offset < total_items:
                next_params = {'limit': self.ITEM_LIMIT, 'offset': current_offset}
                next_url = f"{self.BASE_URL}?{urlencode(next_params)}"
                
                self.logger.debug(f"Schedule crawl offset: {current_offset}/{total_items}")
                
                yield scrapy.Request(
                    url=next_url,
                    method='GET',
                    headers=self.DEFAULT_HEADERS,
                    callback=self.parse_subsequent_response,
                )
                
                current_offset += self.ITEM_LIMIT
        
        else:
            self.logger.info("Crawl complete: All Items retrieved in the first call.")


    def parse_subsequent_response(self, response):
        try:
            data = json.loads(response.text)
        except json.JSONDecodeError:
            self.logger.error(f"JSON Decode error from URL: {response.url}")
            return

        items = data.get('projects', [])
        
        for item in items:
            yield item
