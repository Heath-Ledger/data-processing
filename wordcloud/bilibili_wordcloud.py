import wordcloud
import datetime
import threading
import pymysql
from matplotlib import colors
from PIL import Image
import numpy as np

class DB(object):
    """create mysql conn"""
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

class MyWorldCloud(object):
    def __init__(self):
        self.server_db = DB(host="", username="", pwd="", dbname="").get_instance()
        self.local_db = DB(host="", username="", pwd="", dbname="").get_instance()
        self.yesterday = str(datetime.date.today() + datetime.timedelta(days=-1))
        self.game_name = self.get_game_array()
        self.color_list=['#013E81'] # '#0072E3', #2828F,'#013E81', '#044284', '#2B5F97', '#87A4C4'
        #self.pic_name = "ex.png"
        self.main()

    def create_pic(self, game_name):
        words = []
        #mask = np.array(Image.open(self.pic_name))
        #image_colors = wordcloud.ImageColorGenerator(mask)
        server_db = self.server_db
        server_cursor = server_db.cursor()
        search_user_info_sql = 'select cut_word,sum(today_word_num) as num from cut_bilibili_word where game_name = "{0}" group by cut_word;'.format(game_name["game_name"], self.yesterday)
        server_cursor.execute(search_user_info_sql)
        word_dict = {}
        cut_word_list = server_cursor.fetchall()
        for word_tuple in cut_word_list:
            word_dict[word_tuple[0]] = float(word_tuple[1])
        colormap=colors.ListedColormap(self.color_list)
        w = wordcloud.WordCloud(width=1000, # 宽
                                height=700, # 高
                                #max_font_size=100, # 最大字体大小
                                colormap = colormap, # 颜色数组
                                prefer_horizontal = 1, # 字句不足便会旋转
                                background_color='white', # 图片底色
                                #mask=mask, # 以图片为背景
                                relative_scaling = 0,
                                #color_func=image_colors, # 以图片的颜色为词云的颜色
                                font_path="/home/ubuntu/code/spiders/data/simhei.ttf" )# 字体
        w.generate_from_frequencies(word_dict)
        w.to_file('/home/ubuntu/code/SearchData/static/monitor/img/bilibili/{0}.png'.format(game_name["game_name"]))
        server_cursor.close()

    def get_game_array(self):
        # 获取游戏名数组
        local_db = self.local_db
        local_cursor = local_db.cursor(cursor=pymysql.cursors.DictCursor)
        search_game_array_sql = "select game_name from spider_array where project_name = 'bilibili';"
        local_cursor.execute(search_game_array_sql)
        game_array = local_cursor.fetchall()
        local_cursor.close()
        local_db.close()
        return game_array


    def main(self):
        for game_name in self.game_name:
            try:
                self.create_pic(game_name)
                print(game_name["game_name"] + " --- down")
            except Exception as e:
                print(game_name["game_name"] + " --- failed {0}".format(e))
                continue
                


if __name__ == '__main__':
    yesterday = MyWorldCloud().yesterday
    print(yesterday)
