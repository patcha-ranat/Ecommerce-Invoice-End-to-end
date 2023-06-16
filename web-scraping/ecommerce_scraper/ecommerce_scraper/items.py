# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

from scrapy import Item, Field


class EcommerceScraperItem(Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    shop_id = Field()
    product_id = Field()
    product_name = Field()
    cat_1 = Field()
    cat_2 = Field()
    cat_3 = Field()
    price = Field()
    discount = Field()
    star = Field()
    rating = Field()
    sold = Field()
    color_available = Field()
    size_available = Field()
    stock = Field()
    favorite = Field()
    shop_verified = Field()
    shop_preferred_plus_seller = Field()
    shop_location = Field()
    shop_product = Field()
    shop_response_rate = Field()
    shope_response_time = Field()
    shop_joined = Field()
    shop_followers = Field()
    shop_star = Field()
    shop_rating_bad = Field()
    shop_rating_good = Field()
    shop_rating_normal = Field()