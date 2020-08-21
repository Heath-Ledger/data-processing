import pymysql
import threading
import datetime
import random
import requests
import json
import re
import time
import random
import nltk
from nltk.tokenize import sent_tokenize
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.tag import pos_tag

nltk.download('punkt')
nltk.download('stopwords')
nltk.download('averaged_perceptron_tagger')

class DB(object):
    """创建MySQL实例"""
    def __init__(self, host=None, username=None, pwd=None, dbname=None):
        self.pool = {}
        self.host = host
        self.username = username
        self.pwd = pwd
        self.dbname = dbname

    def get_instance(self):
        name = threading.current_thread().name
        if name not in self.pool:
            conn = pymysql.connect(self.host, self.username, self.pwd, self.dbname)
            self.pool[name] = conn
        return self.pool[name]


class Reddit(object):
    """
       需求：
           1.对reddit内容分词
           2.去除停用词后对剩下数据进行词性标注，取名词数据

    """
    def __init__(self):
        super(Reddit, self).__init__()
        # self.arg = arg
        self.yesterday = str(datetime.date.today() + datetime.timedelta(days=-1)) # 昨日
        self.server_db = DB("ip", "username", "password", "database").get_instance()
        self.server_cursor = self.server_db.cursor(cursor=pymysql.cursors.DictCursor)
        self.contents = []
    
    def GetStopWords(self):
        # 获取停用词
        sql = "select distinct stopword from stopwords;"
        self.server_cursor.execute(sql)
        values = self.server_cursor.fetchall()
        mine_stopwords = []
        for data in values:
            stopword = data["stopword"]
            mine_stopwords.append(stopword)
        english_stopwords = stopwords.words("english")
        mine_stopwords = list(set(english_stopwords + mine_stopwords))
        return mine_stopwords

    @property
    def GetRedditData(self):
        # 获取reddit数据并分词
        sql = """
            select title, video_content from Reddit_search
            where upload_time = '{0}' limit 1000;
        """.format(self.yesterday)
        self.server_cursor.execute(sql)
        values = self.server_cursor.fetchall()
        mine_stopwords = self.GetStopWords()
        for value in values:
            content = ""
            video_content = re.sub("(https?|ftp|file)://[-A-Za-z0-9+&@#/%?=~_|!:,.;]+[-A-Za-z0-9+&@#/%=~_|]", "", value["video_content"])
            video_content = re.sub("'.*?\/.*?\?.*?=.*?&.*'", "", video_content)
            title = re.sub("(https?|ftp|file)://[-A-Za-z0-9+&@#/%?=~_|!:,.;]+[-A-Za-z0-9+&@#/%=~_|]", "", value["title"])

            if not video_content:
                content = title
            else:
                content = video_content
            content = content.lower()
            for line in sent_tokenize(content):
                for word in word_tokenize(line):
                    if word not in mine_stopwords:
                        self.contents.append(word)
        fina_array = []
        
        # 词性标注并筛选名词
        for data1, data2 in pos_tag(self.contents):
            if data2 in ("NN", "NNS", "NNP", "NNPS"):
                fina_array.append(data1)

        print(fina_array)

            
        self.server_cursor.close()
        self.server_db.close()

if __name__ == '__main__':
    # Bilibili()
   Reddit().GetRedditData
    
