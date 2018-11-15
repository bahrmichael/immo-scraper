# spider to collect links to exposes

import scrapy
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Document, Date, Integer, Keyword, Text
from elasticsearch_dsl.connections import connections

connections.create_connection(hosts=["your-elastic-cluster"], timeout=20)

class Expose(Document):
    link = Keyword()
    body = Text()
    tags = Keyword()
    discovered = Date()
    status = Keyword()
    source = Keyword()

    class Index:
        name = 'your-index-name'
        settings = {
          "number_of_shards": 1,
        }

    def save(self, ** kwargs):
        return super(Expose, self).save(** kwargs)

class BlogSpider(scrapy.Spider):

    name = 'immospider'
    base_url = 'https://www.immonet.de/immobiliensuche/sel.do?&sortby=0&suchart=1&objecttype=1&marketingtype=1&parentcat=1&fromarea=40&city='
    # berlin, hamburg, muenchen, koeln, duesseldorf, mannheim, frankfurt
    start_urls = [base_url + '87372', base_url + '109447', base_url + '121673', base_url + '113144', base_url + '100207', base_url + '120884', base_url + '105043' ,
    # stuttgart, dortmund, essen, leipzig, bremen, dresden
                    base_url + '143262', base_url + '99990', base_url + '102157', base_url + '116172', base_url + '88038', base_url + '100051']
    added_urls = []

    def __init__(self, category=None, *args, **kwargs):
        super(BlogSpider, self).__init__(*args, **kwargs)
        self.client = Elasticsearch()
        # use .init() once to create the index from the above defined model
        #Expose.init()

    def parse(self, response):

        # check all links, and collect them if they contain '/angebot/'
        for link in response.css('a'):
            lnk = link.css('::attr(href)').extract()
            if len(lnk) > 0:
                link_text = str(lnk[0])
                # links that contain '/angebot/' link to exposes as long as there is no '?' which leads to other things
                if '/angebot/' in link_text and link_text not in self.added_urls and '?' not in link_text:
                    print(link_text)
                    self.added_urls.append(link_text)
                    expose = Expose(link='https://www.immonet.de' + link_text, tags=['test'], source= 'immonet.de', status='fresh', discovered=datetime.now())
                    expose.save()

        # the button which leads to the next page has the classes shown below
        for next_page in response.css('a').css('.text-right.pull-right'):
            print(next_page)
            yield response.follow(next_page, self.parse)
