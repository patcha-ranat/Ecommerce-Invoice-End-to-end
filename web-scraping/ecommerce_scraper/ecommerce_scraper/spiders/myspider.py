import requests
import json
from scrapy import Request, Spider, Selector
from ..items import EcommerceScraperItem
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
# from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from urllib.parse import urljoin
import re


class MySpider(Spider):
    name = "myspider"
    allowed_domains = ['XXXXX.co.th']

    # GET URLs
    main_url = 'https://XXXXX.co.th/api/v4/pages/get_category_tree'
    content = requests.get(main_url)
    main_dict = json.loads(content.text)
    # list of categoreis
    categories = main_dict['data']['category_list']
    # regular shop only (no mall)
    start_urls = []
    for category in categories:
        catid = category['catid']
        name = category['name'].replace(" ", "-")
        children_id = [children['catid'] for children in category['children']]
        # append root page
        url = f"https://XXXXX.co.th/{name}-cat.{catid}"
        start_urls.append(url)
        # append children pages
        for id in children_id:
            for i in range(9):
                url = f"https://XXXXX.co.th/{name}-cat.{catid}.{id}?page={i}"
                start_urls.append(url)

    ## selenium headless option
    # options = Options()
    # options.headless = True
    # driver = webdriver.Chrome(options=options)
    driver = webdriver.Chrome()
    driver.set_window_size(1920, 1080)
    driver.maximize_window()
    driver.get('https://XXXXX.co.th/Men-Clothes-cat.11044945')

    wait = WebDriverWait(driver, 5)

    select_language_xpath = '//*[@class="language-selection__list"]/div[2]'
    wait.until(EC.element_to_be_clickable((By.XPATH, select_language_xpath)))

    actions = ActionChains(driver)
    actions.click(driver.find_element(By.XPATH, select_language_xpath))
    actions.perform()

    # create string of feature that we needed to extract from webpage
    features = [
            'shop_id',
            'product_id',
            'product_name',
            'cat_1',
            'cat_2',
            'cat_3',
            'price',
            'discount',
            'star',
            'rating',
            'sold',
            'color_available',
            'size_available',
            'stock',
            'favorite',
            'shop_verified',
            'shop_preferred_plus_seller',
            'shop_location',
            'shop_product',
            'shop_response_rate',
            'shope_response_time',
            'shop_joined',
            'shop_followers',
            'shop_star',
            'shop_rating_bad',
            'shop_rating_good',
            'shop_rating_normal'
        ]

    def parse(self, response):
        
        wait = WebDriverWait(self.driver, 5)

        self.driver.get(response.url)
        wait.until(EC.presence_of_element_located((By.XPATH, '//*[@class="row XXXXX-search-item-result__items"]/div[last()]')))
        
        # slowly scroll down to the end of the page
        total_height = int(self.driver.execute_script("return document.body.scrollHeight"))
        for i in range(1, total_height, 100):
            self.driver.execute_script("window.scrollTo(0, {});".format(i))

        sel_source = Selector(text=self.driver.page_source)

        products = sel_source.xpath("//div[contains(@class, 'col-xs-2-4 XXXXX-search-item-result__item')]/a/@href").getall()
        for product_url in products:
            url_sub = urljoin(self.main_url, product_url)

            self.driver.execute_script("window.open('');")
            self.driver.switch_to.window(self.driver.window_handles[1])
            self.driver.get(url_sub)

            # wait until elements loaded
            wait.until(EC.element_to_be_clickable((By.XPATH, '//button[@class="btn btn-solid-primary btn--l vQ3lCI"]')))

            # scroll page
            while True :
                for i in range(1, total_height, 100):
                    self.driver.execute_script("window.scrollTo(0, {});".format(i))
                if EC.presence_of_element_located((By.XPATH, '//div[@class="_4Zf0hW PHkozQ"]')) == True:
                    break
                break

            sel_source = Selector(text=self.driver.page_source)

            #define pattern for regular expression
            pattern = re.compile('(?:-i).(\d*).(\d*)')

            # extract the data from the response using CSS or XPath selectors
            shop_id = pattern.search(url_sub).group(1)
            product_id = pattern.search(url_sub).group(2)
            product_name = sel_source.xpath('//div[@class="YPqix5"]/span/text()').get()
            cat = sel_source.xpath('//a[@class="_4Zf0hW PHkozQ"]/text()').getall()
            cat_1 = cat[1]
            cat_2 = cat[2]
            cat_3 = cat[3]

            price_detail_element = sel_source.xpath('//div[@class="flex items-center"]/div/text()').getall()
            price = price_detail_element[0]
            if len(price_detail_element) != 1:
                discount = price_detail_element[1]
            star =  sel_source.xpath('//div[@class="yz-vZm _2qXJwX"]/text()').get()
            rating = sel_source.xpath('//div[@class="yz-vZm"]/text()').get()
            sold = sel_source.xpath('//div[@class="yiMptB"]/text()').get()
            color_available = sel_source.xpath('//div[@class="flex flex-column"]/div[1]/div/button/text()').getall()
            size_available = sel_source.xpath('//div[@class="flex flex-column"]/div[2]/div/button/text()').getall()
            stock = sel_source.xpath('//div[@class="flex flex-column"]/div[last()]/div[2]/div[2]/text()').get().split()[0]
            favorite = sel_source.xpath('//div[@class="_7na4jG"]/text()').getall()[1].split()[1][1:-1]
            # shop info
            shop_info_url = f'https://XXXXX.co.th/api/v4/product/get_shop_info?shopid={shop_id}'
            info = requests.get(shop_info_url)
            info_dict = json.loads(info.text)

            shop_verified = info_dict['data']["is_XXXXX_verified"]
            shop_preferred_plus_seller = info_dict['data']["is_preferred_plus_seller"]
            shop_location = info_dict['data']["shop_location"]
            shop_product = info_dict['data']["item_count"]
            shop_response_rate = info_dict['data']["response_rate"]
            shope_response_time = info_dict['data']["response_time"]
            shop_last_active = info_dict['data']["last_active_time"]
            shop_joined = sel_source.xpath('//div[@class="s1qcwz"]/div[3]/div[1]/span/text()').get().split()
            shop_followers = info_dict['data']["follower_count"]
            shop_star = info_dict['data']["rating_star"]
            shop_rating_bad = info_dict['data']["rating_bad"]
            shop_rating_good = info_dict['data']["rating_good"]
            shop_rating_normal = info_dict['data']["rating_normal"]

            # create an instance of the item
            item = EcommerceScraperItem()

            # assign the extracted data to the item field
            for feature in self.features:
                exec(f"item['{feature}'] = {feature}")

            # yield the item
            yield item

            self.driver.close()
            self.driver.switch_to.window(self.driver.window_handles[0])
        self.driver.quit()