import gevent
from gevent import monkey
monkey.patch_all()
import json
import requests
import time
from bs4 import BeautifulSoup as BS
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pymongo import ReplaceOne
from datetime import datetime

from loginkey import LOGINKEY

class State():
    def __init__(self):
        self.fetched_pages_ctr_global = 0
        self.global_updates = []
        self.failed_url = []
    def getFailedURL(self):
        return self.failed_url
    def addFailedURL(self, url):
        self.failed_url.append(url)
    def resetFailedURL(self):
        self.failed_url = []
    def getUpdates(self):
        return self.global_updates
    def getNumPages(self):
        return self.fetched_pages_ctr_global
    def fetchedOnePg(self):
        self.fetched_pages_ctr_global += 1
    def addUpdates(self, update):
        self.global_updates.append(update)

class Timer():
    def __init__(self, mark="default", report=False):
        self.time = None
        self.mark = mark
        self.report = report
    def reset(self):
        self.time = time.time()
    def elapsed(self):
        cur_time = time.time()
        if self.report:
            print("[{}] time elapsed: {}".format(self.mark, cur_time-self.time))

def dateFormat(date):
    slist = date.split("-")
    y = slist[0]
    m = int(slist[1])
    d = int(slist[2])
    if m < 10:
        m = "0" + str(m)
    if d < 10:
        d = "0" + str(d)
    return "{}-{}-{}".format(y, m ,d)

def fetchPage(url):
    TIMEOUT = 1.
    HDR = {
        'User-agent' : 'neutrinoliu/prevPost_spider'
    }
    try:
        res = requests.get(url=url, headers=HDR, timeout=TIMEOUT)
        if res.status_code == 503:
            return None
    except:
        return None
    return res

def process(url, myState: State, extract):
    res = fetchPage(url)
    if res: # when not timeout or 503
        myState.fetchedOnePg()
        return extract(res, myState) # early stop if page has no tr.topic element
    else:
        myState.addFailedURL(url)
        return True

def mainGroup(myCollection):
    with open("target.json", "r") as tf:
        config = json.load(tf)["group_targets"]
    GROUPS = config["groups"]
    MAX_PAGE = config["max_page"]
    print("- fetching {}, max_page {}".format(GROUPS, MAX_PAGE))

    SLEEP = .5
    CONCURRENT = 5
    CATEGORY = "group"

    def extract(res, myState: State):
        def getId(path):
            return path.split("/")[-1]
        dom = BS(res.content,features="html.parser")
        topics = dom.select('tr.topic')
        if topics == None or len(topics) == 0:
            return False
        for t in topics:
            subject = t.find('td', {"class":'subject'})
            id = getId(subject.find('a').attrs['href'])
            title = subject.text
            author = t.attrs['data-item-user']
            lastpost = t.find('td', {"class":'lastpost'}).text
            myState.addUpdates(ReplaceOne(
                {
                    "id": int(id),
                },
                {
                    "id": int(id),
                    "type": CATEGORY,
                    "tag": TAG,
                    "title": title,
                    "poster": author,
                    "lastpost": dateFormat(lastpost),
                }, upsert=True))
        
        return True

    totalTimer = Timer("total", report=False)

    for TAG in GROUPS:
#        print("- start fetching <{}>".format(TAG))
        URL_PREFIX = "https://bgm.tv/group/" + TAG + "/forum?page="

        myState = State()
        all_pages = [URL_PREFIX + str(tid) for tid in range(MAX_PAGE)]

        while len(all_pages) > 0:
            myState.resetFailedURL()
            nIters = int(len(all_pages)/CONCURRENT) + 1

            totalTimer.reset()
            for gId in range(nIters):
                jobs = [gevent.spawn(process, url, myState, extract) for url in all_pages[gId * CONCURRENT: min(len(all_pages), (gId+1) * CONCURRENT)]]
                gevent.joinall(jobs)
                continue_flag = True
                for j in jobs:
                    continue_flag = continue_flag and j.value
                if not continue_flag:
                    break
                time.sleep(SLEEP)
            totalTimer.elapsed()
            all_pages = myState.getFailedURL()
            # print("fetched {} pages, timeout {}".format(myState.getNumPages(), len(myState.getFailedURL())))
        #print("uploading total {} documents".format(len(myState.getUpdates())))
        ret = myCollection.bulk_write(myState.getUpdates())
        print("<{}> {} documents modified, {} documents upserted".format(TAG, ret.modified_count, ret.upserted_count))

def getMaxSubjectTopicID(targets):
    def maxID(url):
        res = fetchPage(url)
        if res==None:
            return 0
        dom = BS(res.content,features="html.parser")
        topics = dom.find(class_="topic_list").find_all('tr')
        if topics == None:
            return 0
        latest = topics[1]
        max_id = int(latest.find('a').attrs['href'].split('/')[-1])
        return max_id

    jobs = [gevent.spawn(maxID, url) for url in targets]
    gevent.joinall(jobs)
    return max([job.value for job in jobs])

def mainSubject(myCollection):
    with open("target.json", "r") as tf:
        config = json.load(tf)["subject_targets"]

    MAX_TOPIC_ID = getMaxSubjectTopicID(["https://bgm.tv/" + k for k in ["anime", "book", "music", "game", "real"]])
    if MAX_TOPIC_ID == 0:
        return
    MIN_PAGE = config["offset_start"] + MAX_TOPIC_ID
    MAX_PAGE = config["offset_end"] + MAX_TOPIC_ID
    print("- fetching subject topics from {} to {}".format(MIN_PAGE, MAX_PAGE))

    SLEEP = .5
    CONCURRENT = 5
    CATEGORY = "subject"

    # util functions
    def extract(res, myState: State):
        # parser
        def getId(path):
            return path.split("/")[-1]
        # getTag

        dom = BS(res.content,features="html.parser")
        header = dom.find(id='header')
        if header == None:
            return False
        tag = dom.find(id='subject_inner_info').find('a').attrs['href']
        tag = getId(tag)
        id = getId(res.url)
        title = header.text
        topic = dom.find(class_='postTopic')
        author = topic.attrs['data-item-user']
        post_time = topic.find(class_='post_actions').\
                        find(class_='action').text.split(" ")[2]
        post_time = dateFormat(post_time)
        myState.addUpdates(ReplaceOne(
            {
                "id": int(id),
            },
            {
                "id": int(id),
                "type": CATEGORY,
                "tag": tag,
                "title": title,
                "poster": author,
                "lastpost": dateFormat(post_time),
            }, upsert=True))
        
        return True
                        
    totalTimer = Timer("total", report=False)

    URL_PREFIX = "https://bgm.tv/subject/topic/"

    myState = State()
    all_pages = [URL_PREFIX + str(tid) for tid in range(MIN_PAGE, MAX_PAGE+1)]

    while len(all_pages) > 0:
        myState.resetFailedURL()
        nIters = int(len(all_pages)/CONCURRENT) + 1
        totalTimer.reset()
        for gId in range(nIters):
            jobs = [gevent.spawn(process, url, myState, extract) for url in all_pages[gId * CONCURRENT: min(len(all_pages), (gId+1) * CONCURRENT)]]
            gevent.joinall(jobs)
            time.sleep(SLEEP)
        totalTimer.elapsed()
        all_pages = myState.getFailedURL()
        # print("fetched {} pages, timeout {}".format(myState.getNumPages(), len(myState.getFailedURL())))
    ret = myCollection.bulk_write(myState.getUpdates())
    print("<subject_topics> {} documents modified, {} documents upserted".format(ret.modified_count, ret.upserted_count))

def main():
    URL = "mongodb+srv://neutrino:{}@bgmposts.amrllog.mongodb.net/?retryWrites=true&w=majority".format(LOGINKEY)
    client = MongoClient(URL, server_api=ServerApi('1'))
    try:
        client.admin.command('ping')
        print("Connection Established")
    except Exception as e:
        print(e)
        return("Connection Fails, function aborts at ", datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
    mainSubject(client['topics']['subject_topics'])
    mainGroup(client['topics']['group_topics'])
    return True

main()

