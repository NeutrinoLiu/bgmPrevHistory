from spiderClass import Spider
from bs4 import BeautifulSoup as BS
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pymongo import UpdateOne

class Parser:
    def __init__(self, collection):
        self.buffer = []
        self.collection = collection
    def getBuffer(self):
        return self.buffer
    def pushCloud(self):
        #print("pushed {} updates to cloud".format(len(self.buffer)))
        if len(self.buffer) == 0:
            return
        self.collection.bulk_write(self.buffer)
        self.buffer = []
    @staticmethod
    def extractWordsOnly(raw):
        new = raw.replace('\n', ' ')
        new = new.replace('\t', ' ')
        new = ' '.join(new.split())
        if new == '':
            new = ' '
        return new
    def parse(self, res):
        dom = BS(res.content, features='html.parser')
        id = int(res.url.split('/')[-1])
        item_list = dom.select('div.topic_content')
        if len(item_list) == 0:
            return False
        content = Parser.extractWordsOnly(item_list[0].text)

        self.buffer.append(UpdateOne(
            {"id": id},
            {"$set":{"content": content}}
        ))

        if len(self.buffer) % 100 == 0:
            self.pushCloud()
        return True

class Builder:
    def __init__(self, idx):
        self.itr = iter(idx)
    def __call__(self, id='not used'):
        return 'https://bangumi.tv/group/topic/{}'.format(next(self.itr))

def getTargets():
    LOGINKEY = "Lk1bVC3agIRxBpQU"
    URL = "mongodb+srv://neutrino:{}@bgmposts.amrllog.mongodb.net/?retryWrites=true&w=majority".format(LOGINKEY)

    client = MongoClient(URL, server_api=ServerApi('1'))
    try:
        client.admin.command('ping')
        print("Connection Established")
    except Exception as e:
        print(e)
    myCollection =  client['topics']['group_topics']

    ids = myCollection.find(
    {
        "content": {
            "$exists": False,
        }
    }, {
        "_id": 0,
        "id": 1
    }).distinct('id')

    print('total {} topics no contents'.format(len(ids)))
    return myCollection, ids

def main(event, context):
  myColl, TARGETS = getTargets()
  mParser = Parser(myColl)
  mBuilder = Builder(TARGETS)

  config = {
      'UA': 'neutrinoliu/topics_spider',
      'concurrent': 5,
      'timeout' : 10.,
      'interval' : .5,
      'url_builder' : mBuilder,
      'index_start' : 0,
      'index_end' : len(TARGETS),
      'parser' : mParser,
      'early_stop' : False, # early stop when parser return False
  }

  mSpider = Spider(config)

  mSpider.run()

  mParser.pushCloud()

main(None, None)