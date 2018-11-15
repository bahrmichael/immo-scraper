# spider to parse existing links to exposes and extract details about that expose

import scrapy
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Document, Date, Integer, Keyword, Text, Search
from elasticsearch_dsl.connections import connections
import logging

connections.create_connection(hosts=["your-elastic-cluster"], timeout=20)

logging.getLogger('elasticsearch').setLevel(logging.INFO)
logging.getLogger('urllib3').setLevel(logging.INFO)

class Expose(Document):
    link = Keyword()
    tags = Keyword()
    discovered = Date()
    unavailable_since = Date()
    status = Keyword()
    source = Keyword()
    price = Integer()
    area = Integer()
    rooms = Integer()
    year = Integer()
    objectDescription = Text()
    locationDescription = Text()
    otherDescription = Text()

    class Index:
        name = 'your-index-name'
        settings = {
          "number_of_shards": 1,
        }

    def save(self, ** kwargs):
        return super(Expose, self).save(** kwargs)

class BlogSpider(scrapy.Spider):

    name = 'immospider-expose'
    added_urls = []
    expose_data = {}

    def __init__(self, category=None, *args, **kwargs):
        super(BlogSpider, self).__init__(*args, **kwargs)
        # you might have to remove the url here to so that it picks up the default url defined above. i had some instability with the urls.
        self.client = Elasticsearch("your-elastic-cluster")

        print(self.client)

        s = Search(using=self.client, index="your-index-name") \
            .filter("term", source="immonet.de") \
            .filter("term", status="fresh")

        s = s[0:5000]

        response = s.execute()

        self.start_urls = []
        for hit in response:
            self.start_urls.append(hit.link)
            self.expose_data[hit.link] = hit.meta.id

        print(self.expose_data)

        # use .init() once to create the index from the above defined model
        # Expose.init()

    def parse(self, response):

        print(response.url)

        if "objectnotavailable" in str(response.url):
            print('expose is unavailable')
            # response.url is not the same as response.request.meta. the former follows redirects, the latter is the raw original url passed to the spider
            print(response.request.meta['redirect_urls'][0])
            expose = Expose.get(self.expose_data[response.request.meta['redirect_urls'][0]])
            expose.update(status='unavailable', unavailable_since=datetime.now())
        else:
            print('parsing expose')
            expose = Expose(link=response.url, tags=['test'], source= 'immonet.de', status='parsed', discovered=datetime.now())
            expose.meta.id = self.expose_data[response.url]

            raw_price = response.css('#kfpriceValue::text').extract()
            if raw_price is not None and len(raw_price) > 0:
                price = int(raw_price[0].replace('\t','').replace('\n','').split(u'\xa0')[0].split('.')[0].replace(',',''))
                expose.price = price

            raw_area = response.css('#kffirstareaValue::text').extract()
            if raw_area is not None and len(raw_area) > 0:
                area = int(raw_area[0].replace('\t','').replace('\n','').split('m')[0].split('.')[0].split(',')[0])
                expose.area = area

            raw_rooms = response.css('#kfroomsValue::text').extract()
            if raw_rooms is not None and len(raw_rooms) > 0:
                rooms = int(raw_rooms[0].replace('\t','').replace('\n','').split('.')[0].split(',')[0])
                expose.rooms = rooms

            raw_year = response.css('#yearbuild::text').extract()
            if raw_year is not None and len(raw_year) > 0:
                year = int(raw_year[0].replace('\t','').replace('\n',''))
                expose.year = year

            objectText = response.css('#objectDescription::text').extract()
            locationText = response.css('#locationDescription::text').extract()
            otherText = response.css('#otherDescription::text').extract()

            expose.objectDescription = '\n'.join(objectText)
            expose.locationDescription = '\n'.join(locationText)
            expose.otherDescription = '\n'.join(otherText)
            expose.save()
