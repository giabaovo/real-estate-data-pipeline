import scrapy
import json

class MeeyprojectApiSpider(scrapy.Spider):
    name = 'meeyproject_api'
    
    # URL cơ sở: Dùng f-string để dễ dàng thay đổi page và limit
    BASE_URL = 'https://api.meeyproject.com/v1/projects?sortBy=createdAt:desc&page={page}&limit={limit}'
    ITEM_LIMIT = 30 # Giữ nguyên limit mặc định của API cho mỗi trang
    
    def start_requests(self):
        first_page_url = self.BASE_URL.format(page=1, limit=self.ITEM_LIMIT)
        
        yield scrapy.Request(
            url=first_page_url,
            method='GET',
            headers={'Content-Type': 'application/json'},
            callback=self.parse_api_response,
        )

    def parse_api_response(self, response):
        try:
            data = json.loads(response.text)
        except json.JSONDecodeError:
            self.logger.error(f"JSON Decode Error from URL: {response.url}")
            return

        items = data.get('results', [])

        for item in items:
            yield item

        current_page = data.get('page', 1)
        total_pages = data.get('totalPages')
        
        if total_pages is None:
             self.logger.warning("'totalPages' not found in response, only crawling the current page.")
             return

        if current_page < total_pages:
            next_page = current_page + 1
            next_url = self.BASE_URL.format(page=next_page, limit=self.ITEM_LIMIT)
            
            self.logger.info(f"Crawl next page: {next_page}/{total_pages}")
            
            yield scrapy.Request(
                url=next_url,
                method='GET',
                headers={'Content-Type': 'application/json'},
                callback=self.parse_api_response,
            )
        else:
            self.logger.info(f"Crawl complete. Processed {total_pages} pages.")