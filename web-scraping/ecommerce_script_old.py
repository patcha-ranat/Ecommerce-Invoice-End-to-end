from scrapy.crawler import CrawlerProcess

from ecommerce_scraper.ecommerce_scraper.spiders.myspider import MySpider

# from google.cloud import bigquery

# scrapy part
process = CrawlerProcess({
'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
'FEED_FORMAT': 'json',
'FEED_URI': 'data.json'
})
process.crawl(MySpider)
process.start()