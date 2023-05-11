import gevent
import math
import time
from gevent import monkey
monkey.patch_all()
import requests

class Timer():
    def __init__(self, mark="default", report=False):
        self.time = None
        self.mark = mark
        self.report = report
    def reset(self):
        self.time = time.time()
    def elapsed(self, remark=''):
        cur_time = time.time()
        if self.report:
            print("[{}] time elapsed: {} {}".format(self.mark, cur_time-self.time, remark))

class State:
    def __init__(self):
        self.fails = []
        self.success_ctr = 0
    def successOne(self):
        self.success_ctr += 1
    def getSuccess(self):
        return self.success_ctr
    def getFails(self):
        return self.fails
    def appendFails(self, fail):
        self.fails.append(fail)
    def resetFails(self):
        self.fails = []
    def resetSuccess(self):
        self.success_ctr = 0

class Spider:
    def __init__(self, cfg_dict):
        self.cfg = cfg_dict

    def fetchPage(self, url):
        TIMEOUT = self.cfg['timeout']
        HDR = {
            'User-agent' : self.cfg['UA']
        }
        try:
            res = requests.get(url=url, headers=HDR, timeout=TIMEOUT)
            if res.status_code == 503:
                return None
        except:
            return None
        return res
    
    def worker(self, url, parser, myState):
        res = self.fetchPage(url)
        if res: # when 200, early stop when parser return False
            myState.successOne()
            return parser.parse(res)
        else:
            # refetch if fails, no early stop here
            print("fail at " + url)
            myState.appendFails(url)
            return True

    def run(self):
        cfg = self.cfg
        concurrent = cfg['concurrent']
        targets = [cfg['url_builder'](i)
                   for i in range(cfg['index_start'],
                                  cfg['index_end'])]
        parser = cfg['parser']

        mTimer = Timer(report=True)
        mState = State()
        while len(targets) > 0:
            mTimer.reset()
            mState.resetFails()
            mState.resetSuccess()
            nIters = math.ceil(len(targets)/concurrent)
            for epoch in range(nIters):
                jobs = [gevent.spawn(self.worker, url, parser, mState)
                        for url in targets[epoch * concurrent: (epoch+1) * concurrent]]
                gevent.joinall(jobs)
                time.sleep(cfg['interval'])
                if cfg['early_stop']:
                    continue_flag = True
                    for j in jobs:
                        continue_flag = continue_flag and j.value
                    if not continue_flag:
                        break
            mTimer.elapsed()
            targets = mState.getFails()
            print("fetched {}, fails {}".format(mState.getSuccess(), len(targets)))