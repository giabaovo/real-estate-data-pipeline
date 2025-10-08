import scrapy
import json

class MeeyprojectApiSpider(scrapy.Spider):
    """
    Spider thu thập dữ liệu dự án bất động sản từ API Meeyproject.
    Sử dụng kỹ thuật Paging (Phân trang) để xử lý dữ liệu lớn (5000+ items).
    """
    name = 'meeyproject_api'
    
    # URL cơ sở: Dùng f-string để dễ dàng thay đổi page và limit
    BASE_URL = 'https://api.meeyproject.com/v1/projects?sortBy=createdAt:desc&page={page}&limit={limit}'
    ITEM_LIMIT = 30 # Giữ nguyên limit mặc định của API cho mỗi trang
    
    def start_requests(self):
        """
        Khởi tạo request đầu tiên (page=1) để lấy tổng số trang (totalPages).
        """
        first_page_url = self.BASE_URL.format(page=1, limit=self.ITEM_LIMIT)
        
        # Thiết lập request là POST/GET tùy theo yêu cầu của API, ở đây là GET
        yield scrapy.Request(
            url=first_page_url,
            method='GET',
            headers={'Content-Type': 'application/json'},
            callback=self.parse_api_response,
        )

    def parse_api_response(self, response):
        """
        Xử lý phản hồi JSON, trích xuất items và lên lịch cho trang tiếp theo.
        """
        try:
            data = json.loads(response.text)
        except json.JSONDecodeError:
            self.logger.error(f"Lỗi JSON Decode từ URL: {response.url}")
            return

        # 1. Trích xuất Items
        # Giả định items nằm trong key 'projects'
        items = data.get('results', [])
        
        # Lặp qua từng item và đẩy vào Pipeline chung (MinIOLoadPipeline)
        for item in items:
            yield item

        # 2. Xử lý Paging (Lên lịch cho trang tiếp theo)
        current_page = data.get('page', 1)
        total_pages = data.get('totalPages')
        
        if total_pages is None:
             self.logger.warning("Không tìm thấy 'totalPages' trong response, chỉ crawl trang hiện tại.")
             return

        # Nếu trang hiện tại nhỏ hơn tổng số trang, lên lịch cho trang tiếp theo
        if current_page < total_pages:
            next_page = current_page + 1
            next_url = self.BASE_URL.format(page=next_page, limit=self.ITEM_LIMIT)
            
            self.logger.info(f"Lên lịch crawl trang tiếp theo: {next_page}/{total_pages}")
            
            yield scrapy.Request(
                url=next_url,
                method='GET',
                headers={'Content-Type': 'application/json'},
                callback=self.parse_api_response,
            )
        else:
            self.logger.info(f"Hoàn tất crawl. Đã xử lý {total_pages} trang.")