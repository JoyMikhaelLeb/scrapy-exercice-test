import scrapy


class WplaceItem(scrapy.Item):
    identifier     = scrapy.Field()
    title          = scrapy.Field()
    description    = scrapy.Field()
    ref_no         = scrapy.Field()
    published_date = scrapy.Field()
    body           = scrapy.Field()
    doc_url        = scrapy.Field()
    file_path      = scrapy.Field()
    file_hash      = scrapy.Field()
    partition_date = scrapy.Field()