import requests
import json
from scrapy import Selector
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from urllib.parse import urljoin
import re
import time


def get_proxies():
    url = 'https://free-proxy-list.net/'
    response = requests.get(url)
    proxies_source = Selector(text=response.text)
    proxies = set()
    for element in proxies_source.xpath('//tbody/tr'):
        if element.xpath('.//td[7][contains(text(),"yes")]'):
            #Grabbing IP and corresponding PORT
            proxy = ":".join([element.xpath('.//td[1]/text()').get(), element.xpath('.//td[2]/text()').get()])
            proxies.add(proxy)
    return list(proxies)

def get_default_option(headless=False):
    chrome_options = Options()
    # selenium non-headless/headless option
    #options.headless = True
    chrome_options.headless = headless
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
    chrome_options.add_argument("--user-agent={}".format(user_agent))
    return chrome_options

def start_new_selenium_session(url_new_session, headless=False, custom_proxies_list=[]):
    
    # if custom proxies are provided
    if len(custom_proxies_list) != 0:
        for i in range(len(custom_proxies_list)):
            chrome_options = get_default_option(headless)
            # get a proxy from the pool
            print("Proxy #%d"%i)
            custom_proxy = custom_proxies_list[i]
            chrome_options.add_argument("--proxy-server={}".format(custom_proxy))
            try:
                driver = webdriver.Chrome(options=chrome_options)
                driver.set_window_size(1920, 1080)
                driver.maximize_window()
                driver.get(url_new_session)

                wait = WebDriverWait(driver, 4)

                select_language_xpath = '//*[@class="language-selection__list"]/div[2]'
                wait.until(EC.element_to_be_clickable((By.XPATH, select_language_xpath)))

                actions = ActionChains(driver)
                actions.click(driver.find_element(By.XPATH, select_language_xpath))
                actions.perform()

                return driver
            except:
                print("Skipping. Connnection error")
                driver.quit()
    
    # no custom proxies are provided or all proxies in custom_proxies_list are used
    chrome_options = get_default_option(headless)
    driver = webdriver.Chrome(options=chrome_options)
    driver.set_window_size(1920, 1080)
    driver.maximize_window()
    driver.get(url_new_session)

    wait = WebDriverWait(driver, 4)

    select_language_xpath = '//*[@class="language-selection__list"]/div[2]'
    wait.until(EC.element_to_be_clickable((By.XPATH, select_language_xpath)))

    actions = ActionChains(driver)
    actions.click(driver.find_element(By.XPATH, select_language_xpath))
    actions.perform()

    return driver

def write_to_json(lst: list, fn: str):
    # with open(fn, 'a', encoding='utf-8') as file:
    with open(fn, 'w', encoding='utf-8') as file:
        for item in lst:
            x = json.dumps(item, ensure_ascii=False ,indent=4)
            file.write(x + '\n')
#export to JSON
# write_to_json(data, 'elements.json')

# --------------------------------------------------------------------
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
        for i in range(5):
            url = f"https://XXXXX.co.th/{name}-cat.{catid}.{id}?page={i}"
            start_urls.append(url)

proxies_list = get_proxies()
driver = start_new_selenium_session(url_new_session='https://XXXXX.co.th/Men-Clothes-cat.11044945', headless=False)

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
    'product_reviews',
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

data = list()
product_number = 1

for url in start_urls:
    driver.get(url)
    wait = WebDriverWait(driver, 4)
    wait.until(EC.presence_of_element_located((By.XPATH, '//*[@class="row XXXXX-search-item-result__items"]/div[last()]')))
    
    # slowly scroll down to the end of the page
    total_height = int(driver.execute_script("return document.body.scrollHeight"))
    for i in range(1, total_height, 100):
        driver.execute_script("window.scrollTo(0, {});".format(i))

    sel_source = Selector(text=driver.page_source)

    products = sel_source.xpath("//div[contains(@class, 'col-xs-2-4 XXXXX-search-item-result__item')]/a/@href").getall()
    for product_url in products:
        url_sub = urljoin(main_url, product_url)

        driver.execute_script("window.open('');")
        driver.switch_to.window(driver.window_handles[1])
        driver.get(url_sub)

        # wait until elements loaded
        # wait.until(EC.presence_of_element_located((By.XPATH, '//button[@class="btn btn-solid-primary btn--l vQ3lCI"]')))
        # at this point, login page would occur if bot detected
        retry = True
        while retry == True:
            try:
                wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="main"]/div/div[2]/div[1]/div/div/div/div[2]/div[3]/div/div[5]/div/div/button[2]')))
                retry = False
            except:
                driver.quit()
                print("Bot Detected... retrying")
                # time.sleep(61)
                proxies_list = get_proxies()
                driver = start_new_selenium_session(url, headless=False)
                driver.execute_script("window.open('');")
                driver.switch_to.window(driver.window_handles[1])
                driver.get(url_sub)
                wait = WebDriverWait(driver, 10)
                
        # scroll page
        while True :
            for i in range(1, total_height, 100):
                driver.execute_script("window.scrollTo(0, {});".format(i))
            if EC.presence_of_element_located((By.XPATH, '//div[@class="_4Zf0hW PHkozQ"]')) == True:
                break
            break

        sel_source = Selector(text=driver.page_source)

        #define pattern for regular expression
        pattern = re.compile('(?:-i).(\d*).(\d*)')

        # extract the data from the response using CSS or XPath selectors
        shop_id = pattern.search(url_sub).group(1)
        product_id = pattern.search(url_sub).group(2)
        product_name = sel_source.xpath('//div[@class="YPqix5"]/span/text()').get()
        cat = sel_source.xpath('//a[@class="_4Zf0hW PHkozQ"]/text()').getall()
        cat_1 = cat[1]
        try:
            cat_2 = cat[2]
        except:
            cat_2 = ""
        try:
            cat_3 = cat[3]
        except:
            cat_3 = ""

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

        # extract reviews: [number_of_review, comment, likes]
        comments = sel_source.xpath('//div[@class="EXI9SU"]/text()').getall()
        likes = sel_source.xpath('//div[@class="XXXXX-product-rating__like-count"]/text()').getall()
        number_reviews = len(sel_source.xpath('//div[@class="XXXXX-product-comment-list"]/div').getall())
        product_reviews = [[i, comments[i], likes[i]] for i in range(number_reviews)]

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

        ## create an instance of the item
        # item = EcommerceScraperItem()
        item = dict()

        # assign the extracted data to the item field
        for feature in features:
            exec(f"item['{feature}'] = {feature}")
        
        ## check each extracted product 
        # print(item)
        data.append(item)
        print(f"extracted product: {product_number}")
        product_number += 1

        ## write json after each product extracted
        write_to_json([data], 'data.json')

        driver.close()
        driver.switch_to.window(driver.window_handles[0])
driver.quit()

print("scraping completed")
print(f"{product_number} products are extracted")

# write json after all product extracted
write_to_json([data], 'data.json')